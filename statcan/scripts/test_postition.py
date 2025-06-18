import psycopg2
from psycopg2.extras import DictCursor
from statcan.tools.config import DB_CONFIG


def check_dimension_positions():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=DictCursor)

    # Fetch positions per productid
    cur.execute("""
        SELECT
            productid,
            array_agg(dimension_position ORDER BY dimension_position) AS positions
        FROM cube.cube_dimension_map
        GROUP BY productid
    """)

    bad = []
    for row in cur:
        pid = row['productid']
        positions = row['positions']
        # Expected is [1, 2, ..., len(positions)]
        expected = list(range(1, len(positions) + 1))
        if positions != expected:
            bad.append((pid, positions))

    cur.close()
    conn.close()

    if bad:
        print("Products with non-sequential dimension_position:")
        for pid, positions in bad:
            print(f"  • {pid}: {positions}")
    else:
        print("✔ All productids have dimension_position starting at 1 and sequential.")

if __name__ == "__main__":
    check_dimension_positions()

