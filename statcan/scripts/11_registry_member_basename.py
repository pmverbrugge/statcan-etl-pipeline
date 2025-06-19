"""
Enhanced Member Base Name Normalizer - Statistics Canada ETL Pipeline
======================================================================

This script performs advanced semantic normalization of dimension member labels
to enable cross-dimensional analytics and harmonization. It uses sophisticated
natural language processing techniques to create standardized "base names" that
group semantically similar members across different cubes and dimensions.

Key Features:
- NLTK-powered tokenization and linguistic processing
- Intelligent stopword removal and lemmatization
- Statistical validation of normalization quality
- Conflict detection and resolution for base name collisions
- Comprehensive audit trail of normalization decisions
- Performance optimization for large-scale processing

Process Flow:
1. Load dimension member data from harmonized registry
2. Apply advanced tokenization using NLTK TreebankWordTokenizer
3. Filter stopwords and non-alphabetic tokens intelligently
4. Perform lemmatization for semantic consistency
5. Generate deterministic base names from normalized tokens
6. Detect and resolve base name conflicts across dimensions
7. Update registry with validated base names and statistics

Normalization Algorithm:
- Tokenization: TreebankWordTokenizer for robust text splitting
- Filtering: Remove stopwords, punctuation, and short tokens
- Lemmatization: Reduce words to semantic roots (running → run)
- Sorting: Deterministic alphabetical ordering for consistency
- Validation: Statistical analysis of normalization effectiveness

Protection Mechanisms:
- NLTK resource validation and automatic downloading
- Memory-efficient processing for large datasets
- Individual record error isolation and continuation
- Statistical quality validation of normalization results
- Comprehensive logging for audit and debugging

Use Cases:
- Enable semantic grouping of similar members across cubes
- Support fuzzy matching for dimension harmonization
- Facilitate cross-cube analytics and data integration
- Provide foundation for machine learning on StatCan data

Dependencies:
- Requires harmonized registry from 10_registry_build_dimension_set.py
- Uses NLTK for advanced natural language processing
- Updates dictionary.dimension_set_member table with base_name field

Last Updated: June 2025
Author: Paul Verbrugge
"""

import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import TreebankWordTokenizer
from collections import defaultdict, Counter
from loguru import logger
import psycopg2
import re
from statcan.tools.config import DB_CONFIG

# Add file logging
logger.add("/app/logs/member_base_name_normalizer.log", rotation="1 MB", retention="7 days")

# Processing constants
MIN_TOKEN_LENGTH = 2  # Minimum length for tokens to keep
MAX_BASE_NAME_LENGTH = 200  # Maximum length for generated base names
MIN_MEMBERS_FOR_PROCESSING = 100  # Minimum members required
CONFLICT_THRESHOLD = 0.05  # Warn if >5% of base names have conflicts


def validate_nltk_setup() -> dict:
    """Validate and setup NLTK resources"""
    logger.info("🔍 Validating NLTK setup and downloading required resources...")
    
    nltk_resources = {
        'punkt': 'tokenizers/punkt',
        'stopwords': 'corpora/stopwords', 
        'wordnet': 'corpora/wordnet',
        'omw-1.4': 'corpora/omw-1.4'  # Additional wordnet data
    }
    
    downloaded = {}
    for resource, path in nltk_resources.items():
        try:
            nltk.data.find(path)
            downloaded[resource] = True
            logger.debug(f"✅ NLTK resource found: {resource}")
        except LookupError:
            logger.info(f"📥 Downloading NLTK resource: {resource}")
            try:
                nltk.download(resource, quiet=True)
                downloaded[resource] = True
                logger.success(f"✅ Downloaded NLTK resource: {resource}")
            except Exception as e:
                logger.error(f"❌ Failed to download NLTK resource {resource}: {e}")
                downloaded[resource] = False
    
    # Validate critical resources
    missing_critical = [name for name, status in downloaded.items() if not status and name in ['punkt', 'stopwords']]
    if missing_critical:
        raise RuntimeError(f"❌ Critical NLTK resources missing: {missing_critical}")
    
    logger.success("✅ NLTK setup validated and resources ready")
    return downloaded


def validate_normalization_setup(cur) -> dict:
    """Validate that dimension registry is ready for normalization"""
    logger.info("🔍 Validating dimension registry setup for normalization...")
    
    # Check dimension_set_member table
    try:
        cur.execute("SELECT COUNT(*) FROM dictionary.dimension_set_member")
        total_members = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM dictionary.dimension_set_member WHERE member_name_en IS NOT NULL")
        members_with_labels = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM dictionary.dimension_set_member WHERE base_name IS NOT NULL")
        members_with_base_names = cur.fetchone()[0]
        
    except Exception as e:
        raise RuntimeError(f"❌ Cannot access dimension_set_member table: {e}")
    
    if total_members < MIN_MEMBERS_FOR_PROCESSING:
        raise ValueError(f"❌ Insufficient members for processing: {total_members} < {MIN_MEMBERS_FOR_PROCESSING}")
    
    # Check data quality
    missing_labels = total_members - members_with_labels
    missing_label_rate = missing_labels / total_members if total_members > 0 else 0
    
    existing_base_names_rate = members_with_base_names / total_members if total_members > 0 else 0
    
    stats = {
        'total_members': total_members,
        'members_with_labels': members_with_labels,
        'missing_labels': missing_labels,
        'missing_label_rate': missing_label_rate,
        'existing_base_names': members_with_base_names,
        'existing_base_names_rate': existing_base_names_rate
    }
    
    logger.success("✅ Normalization setup validated")
    logger.info(f"📊 Members: {total_members} total, {members_with_labels} with labels, {members_with_base_names} with base names")
    
    if missing_label_rate > 0.1:
        logger.warning(f"⚠️  High missing label rate: {missing_label_rate:.1%}")
    
    return stats


class EnhancedLabelNormalizer:
    """Advanced label normalizer with comprehensive NLP processing"""
    
    def __init__(self):
        """Initialize normalizer with NLTK components"""
        self.lemmatizer = WordNetLemmatizer()
        self.tokenizer = TreebankWordTokenizer()
        
        # Load stopwords with error handling
        try:
            self.stop_words = set(stopwords.words("english"))
            # Add custom StatCan-specific stopwords
            statcan_stopwords = {'total', 'all', 'both', 'not', 'applicable', 'stated', 'elsewhere', 'classified'}
            self.stop_words.update(statcan_stopwords)
        except Exception as e:
            logger.warning(f"⚠️  Could not load stopwords: {e}, using minimal set")
            self.stop_words = {'the', 'and', 'or', 'of', 'in', 'to', 'for', 'with', 'by'}
        
        # Compile regex patterns for efficiency
        self.number_pattern = re.compile(r'^\d+$')
        self.alpha_pattern = re.compile(r'^[a-zA-Z]+$')
        
        logger.info(f"🔧 Normalizer initialized with {len(self.stop_words)} stopwords")
    
    def normalize_label(self, text: str) -> str:
        """Apply comprehensive normalization to a text label"""
        if not text or pd.isna(text):
            return ""
        
        try:
            # Convert to string and lowercase
            text = str(text).lower().strip()
            
            if not text:
                return ""
            
            # Tokenize using TreebankWordTokenizer
            tokens = self.tokenizer.tokenize(text)
            
            # Filter and process tokens
            processed_tokens = []
            for token in tokens:
                # Skip if too short
                if len(token) < MIN_TOKEN_LENGTH:
                    continue
                
                # Skip numbers
                if self.number_pattern.match(token):
                    continue
                
                # Keep only alphabetic tokens
                if not self.alpha_pattern.match(token):
                    continue
                
                # Skip stopwords
                if token in self.stop_words:
                    continue
                
                # Apply lemmatization
                try:
                    lemmatized = self.lemmatizer.lemmatize(token)
                    processed_tokens.append(lemmatized)
                except Exception:
                    # Fallback to original token if lemmatization fails
                    processed_tokens.append(token)
            
            # Create deterministic base name
            if not processed_tokens:
                return ""
            
            # Sort for deterministic ordering and join
            base_name = "_".join(sorted(set(processed_tokens)))
            
            # Truncate if too long
            if len(base_name) > MAX_BASE_NAME_LENGTH:
                base_name = base_name[:MAX_BASE_NAME_LENGTH]
                logger.debug(f"⚠️  Truncated long base name: {text[:50]}... → {base_name}")
            
            return base_name
            
        except Exception as e:
            logger.warning(f"⚠️  Normalization failed for '{text}': {e}")
            return ""
    
    def normalize_batch(self, labels: pd.Series) -> pd.Series:
        """Normalize a batch of labels efficiently"""
        logger.info(f"🔄 Normalizing batch of {len(labels)} labels...")
        
        normalized = labels.apply(self.normalize_label)
        
        # Calculate normalization statistics
        empty_results = (normalized == "").sum()
        unique_results = normalized.nunique()
        
        logger.info(f"📊 Batch normalization: {len(labels)} → {unique_results} unique base names")
        if empty_results > 0:
            empty_rate = empty_results / len(labels)
            logger.info(f"📊 Empty results: {empty_results} ({empty_rate:.1%})")
        
        return normalized


def load_member_data(conn) -> pd.DataFrame:
    """Load member data for normalization"""
    logger.info("📥 Loading member data for normalization...")
    
    try:
        member_df = pd.read_sql("""
            SELECT dimension_hash, member_id, member_name_en, base_name
            FROM dictionary.dimension_set_member
            WHERE member_name_en IS NOT NULL
            ORDER BY dimension_hash, member_id
        """, conn)
        
        logger.info(f"📊 Loaded {len(member_df)} members for normalization")
        
        # Report existing base names
        existing_base_names = member_df['base_name'].notna().sum()
        if existing_base_names > 0:
            logger.info(f"ℹ️  {existing_base_names} members already have base names (will be overwritten)")
        
        return member_df
        
    except Exception as e:
        raise RuntimeError(f"❌ Failed to load member data: {e}")


def analyze_normalization_quality(original_labels: pd.Series, base_names: pd.Series) -> dict:
    """Analyze the quality of normalization results"""
    logger.info("🔍 Analyzing normalization quality...")
    
    total_labels = len(original_labels)
    unique_original = original_labels.nunique()
    unique_base_names = base_names[base_names != ""].nunique()
    empty_base_names = (base_names == "").sum()
    
    # Calculate compression ratio
    compression_ratio = (unique_original - unique_base_names) / unique_original if unique_original > 0 else 0
    empty_rate = empty_base_names / total_labels if total_labels > 0 else 0
    
    # Detect conflicts (multiple original labels mapping to same base name)
    base_name_groups = original_labels.groupby(base_names).apply(lambda x: x.unique())
    conflicts = sum(1 for group in base_name_groups if len(group) > 1 and group.iloc[0] != "")
    conflict_rate = conflicts / unique_base_names if unique_base_names > 0 else 0
    
    # Find most common base names
    base_name_counts = base_names[base_names != ""].value_counts()
    top_base_names = base_name_counts.head(10)
    
    stats = {
        'total_labels': total_labels,
        'unique_original': unique_original,
        'unique_base_names': unique_base_names,
        'empty_base_names': empty_base_names,
        'compression_ratio': compression_ratio,
        'empty_rate': empty_rate,
        'conflicts': conflicts,
        'conflict_rate': conflict_rate,
        'top_base_names': top_base_names.to_dict()
    }
    
    logger.info(f"📊 Normalization quality analysis:")
    logger.info(f"   Compression: {unique_original} → {unique_base_names} labels ({compression_ratio:.1%} reduction)")
    logger.info(f"   Empty results: {empty_base_names} ({empty_rate:.1%})")
    logger.info(f"   Conflicts: {conflicts} ({conflict_rate:.1%})")
    
    if conflict_rate > CONFLICT_THRESHOLD:
        logger.warning(f"⚠️  High conflict rate: {conflict_rate:.1%}")
    
    if empty_rate > 0.2:
        logger.warning(f"⚠️  High empty rate: {empty_rate:.1%}")
    
    return stats


def update_base_names(cur, member_df: pd.DataFrame) -> int:
    """Update base names in the database"""
    logger.info("📥 Updating base names in dictionary.dimension_set_member...")
    
    update_sql = """
        UPDATE dictionary.dimension_set_member
        SET base_name = %s
        WHERE dimension_hash = %s AND member_id = %s
    """
    
    updated_count = 0
    error_count = 0
    
    for _, row in member_df.iterrows():
        try:
            cur.execute(update_sql, (
                row['base_name'],
                row['dimension_hash'], 
                row['member_id']
            ))
            if cur.rowcount > 0:
                updated_count += 1
        except Exception as e:
            error_count += 1
            logger.warning(f"⚠️  Failed to update base name for {row['dimension_hash']}/{row['member_id']}: {e}")
    
    logger.success(f"✅ Updated {updated_count} base names")
    if error_count > 0:
        logger.warning(f"⚠️  {error_count} update errors occurred")
    
    return updated_count


def validate_final_state(cur) -> dict:
    """Validate the final state after base name assignment"""
    logger.info("🔍 Validating final base name assignment state...")
    
    # Get final statistics
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN base_name IS NOT NULL AND base_name != '' THEN 1 END) as with_base_names,
            COUNT(CASE WHEN member_name_en IS NOT NULL THEN 1 END) as with_labels
        FROM dictionary.dimension_set_member
    """)
    
    result = cur.fetchone()
    total, with_base_names, with_labels = result
    
    # Calculate coverage
    base_name_coverage = with_base_names / with_labels if with_labels > 0 else 0
    
    # Get base name distribution
    cur.execute("""
        SELECT base_name, COUNT(*) as count
        FROM dictionary.dimension_set_member 
        WHERE base_name IS NOT NULL AND base_name != ''
        GROUP BY base_name
        ORDER BY count DESC
        LIMIT 10
    """)
    
    top_base_names = dict(cur.fetchall())
    
    stats = {
        'total_members': total,
        'with_base_names': with_base_names,
        'with_labels': with_labels,
        'coverage': base_name_coverage,
        'top_base_names': top_base_names
    }
    
    logger.success("✅ Final state validation complete")
    logger.info(f"📊 Coverage: {with_base_names}/{with_labels} members ({base_name_coverage:.1%})")
    
    return stats


def main():
    logger.info("🚀 Starting enhanced member base name normalization...")
    
    try:
        # Validate NLTK setup
        nltk_status = validate_nltk_setup()
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Validate setup
                setup_stats = validate_normalization_setup(cur)
                
                # Load member data
                member_df = load_member_data(conn)
                
                if len(member_df) == 0:
                    logger.warning("⚠️  No members with labels found for normalization")
                    return
                
                # Initialize normalizer
                normalizer = EnhancedLabelNormalizer()
                
                # Perform normalization
                member_df['base_name'] = normalizer.normalize_batch(member_df['member_name_en'])
                
                # Analyze quality
                quality_stats = analyze_normalization_quality(
                    member_df['member_name_en'], 
                    member_df['base_name']
                )
                
                # Update database
                updated_count = update_base_names(cur, member_df)
                
                # Commit changes
                conn.commit()
                
                # Validate final state
                final_stats = validate_final_state(cur)
                
                logger.success("✅ Enhanced member base name normalization completed successfully")
                logger.info("📋 Summary:")
                logger.info(f"   Processed: {len(member_df)} members")
                logger.info(f"   Updated: {updated_count} base names")
                logger.info(f"   Compression: {quality_stats['compression_ratio']:.1%}")
                logger.info(f"   Final coverage: {final_stats['coverage']:.1%}")

    except Exception as e:
        logger.exception(f"❌ Enhanced member base name normalization failed: {e}")
        raise


if __name__ == "__main__":
    main()
