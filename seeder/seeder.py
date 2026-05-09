"""
E-commerce data seeder.

Continuously inserts and updates rows in MySQL to drive CDC events.
Generates realistic patterns:
  - New orders (primary CDC feed)
  - Order status updates (update events)
  - Occasional product stock changes (update events)
  - Rare customer tier upgrades (update events)
"""
from __future__ import annotations

import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime

import mysql.connector
from mysql.connector import Error


def connect(retries: int = 30) -> mysql.connector.MySQLConnection:
    cfg = dict(
        host=os.environ.get("MYSQL_HOST", "mysql"),
        user=os.environ.get("MYSQL_USER", "debezium"),
        password=os.environ.get("MYSQL_PASSWORD", "debezium"),
        database=os.environ.get("MYSQL_DB", "ecommerce"),
    )
    for attempt in range(retries):
        try:
            conn = mysql.connector.connect(**cfg)
            print(f"[seeder] connected to MySQL", flush=True)
            return conn
        except Error as e:
            print(f"[seeder] MySQL not ready ({attempt+1}/{retries}): {e}", flush=True)
            time.sleep(2)
    raise RuntimeError("Cannot connect to MySQL")


def get_ids(conn) -> tuple[list[int], list[int]]:
    cur = conn.cursor()
    cur.execute("SELECT id FROM customers")
    customers = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM products")
    products = [r[0] for r in cur.fetchall()]
    cur.close()
    return customers, products


def place_order(conn, customers: list[int], products: list[int]) -> int | None:
    try:
        cur = conn.cursor()
        c_id = random.choice(customers)
        p_id = random.choice(products)
        cur.execute("SELECT price FROM products WHERE id = %s", (p_id,))
        row = cur.fetchone()
        if not row:
            return None
        price = float(row[0])
        qty   = random.randint(1, 3)
        ref   = str(uuid.uuid4())
        channel = random.choice(['WEB', 'MOBILE', 'STORE'])

        cur.execute("""
            INSERT INTO orders (order_ref, customer_id, product_id, quantity, unit_price, currency, channel)
            VALUES (%s, %s, %s, %s, %s, 'AED', %s)
        """, (ref, c_id, p_id, qty, price, channel))
        conn.commit()
        order_id = cur.lastrowid
        cur.close()
        return order_id
    except Error as e:
        print(f"[seeder] order insert error: {e}", flush=True)
        conn.rollback()
        return None


def advance_order_status(conn) -> None:
    """Move a random PLACED order to CONFIRMED or SHIPPED."""
    try:
        cur = conn.cursor()
        transitions = {
            'PLACED': 'CONFIRMED',
            'CONFIRMED': 'SHIPPED',
            'SHIPPED': 'DELIVERED',
        }
        for from_status, to_status in transitions.items():
            cur.execute("""
                UPDATE orders SET status = %s
                WHERE id = (
                    SELECT id FROM (
                        SELECT id FROM orders WHERE status = %s ORDER BY RAND() LIMIT 1
                    ) t
                )
            """, (to_status, from_status))
        conn.commit()
        cur.close()
    except Error as e:
        conn.rollback()


def update_stock(conn, products: list[int]) -> None:
    try:
        cur = conn.cursor()
        p_id = random.choice(products)
        delta = random.choice([-1, -2, -3, 10, 20])
        cur.execute("""
            UPDATE products SET stock = GREATEST(0, stock + %s) WHERE id = %s
        """, (delta, p_id))
        conn.commit()
        cur.close()
    except Error as e:
        conn.rollback()


def main() -> None:
    eps    = float(os.environ.get("EVENTS_PER_SECOND", "5"))
    conn   = connect()
    customers, products = get_ids(conn)

    stop = {"now": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(now=True))
    signal.signal(signal.SIGINT,  lambda *_: stop.update(now=True))

    interval  = 1.0 / eps
    next_tick = time.time()
    total     = 0

    print(f"[seeder] generating ~{eps} events/sec "
          f"({len(customers)} customers, {len(products)} products)", flush=True)

    while not stop["now"]:
        roll = random.random()
        if roll < 0.65:
            place_order(conn, customers, products)
        elif roll < 0.85:
            advance_order_status(conn)
        elif roll < 0.95:
            update_stock(conn, products)
        # else: no-op tick

        total += 1
        if total % 100 == 0:
            print(f"[seeder] {total} events generated", flush=True)

        next_tick += interval
        sleep = next_tick - time.time()
        if sleep > 0:
            time.sleep(sleep)
        else:
            next_tick = time.time()

    conn.close()
    print(f"[seeder] stopped after {total} events", flush=True)


if __name__ == "__main__":
    sys.exit(main())
