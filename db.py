import mysql.connector
from mysql.connector import Error
import hashlib

# =========================
# Connection Config
# =========================
DB_CONFIG = {
    "host": "localhost",
    "user": "",          # your MySQL username
    "password": "",  # your MySQL password
    "database": "event_catalog",
    "auth_plugin": "mysql_native_password",  # safe default
}

DB_NAME = DB_CONFIG["database"]


# =========================
# Low-level connections
# =========================
def _connect_without_db():
    """Connect to MySQL server without specifying a database."""
    cfg = DB_CONFIG.copy()
    cfg.pop("database", None)
    return mysql.connector.connect(**cfg)

def _connect_with_db():
    """Connect to MySQL server specifying the target database."""
    return mysql.connector.connect(**DB_CONFIG)


# =========================
# Ensure DB exists
# =========================
def ensure_database():
    """Create the database if it does not exist."""
    try:
        conn = _connect_without_db()
        cur = conn.cursor()
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
            "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        conn.commit()
        cur.close(); conn.close()
        print(f"🧱 Database ready: {DB_NAME}")
        return True
    except Error as e:
        print(f"❌ Unable to create database '{DB_NAME}': {e}")
        return False


# =========================
# Public connection helper
# =========================
def create_connection():
    """
    Return a connection to the target database.
    If the DB is missing, create it and retry.
    """
    try:
        # First try connecting directly (fast path)
        conn = _connect_with_db()
        if conn.is_connected():
            return conn
    except Error as e:
        # If unknown database, create it then retry
        if getattr(e, "errno", None) == 1049 or "Unknown database" in str(e):
            if ensure_database():
                try:
                    conn = _connect_with_db()
                    if conn.is_connected():
                        return conn
                except Error as e2:
                    print(f"❌ Error connecting after DB create: {e2}")
        else:
            print(f"❌ Error connecting to MySQL: {e}")
    return None


# =========================
# Schema
# =========================
def create_tables():
    """Create all required tables (idempotent)."""
    conn = create_connection()
    if not conn:
        print("❌ Could not connect to database.")
        return
    cursor = conn.cursor()

    organizers_table = """
    CREATE TABLE IF NOT EXISTS organizers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        uid VARCHAR(64) UNIQUE,
        name VARCHAR(255),
        ein VARCHAR(20),
        website VARCHAR(255),
        email VARCHAR(255),
        phone VARCHAR(50),
        contact_name VARCHAR(255),
        contact_title VARCHAR(255),
        contact_email VARCHAR(255),
        facebook VARCHAR(255),
        instagram VARCHAR(255),
        UNIQUE (uid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    events_table = """
    CREATE TABLE IF NOT EXISTS events (
        id INT AUTO_INCREMENT PRIMARY KEY,
        uid VARCHAR(64) UNIQUE,
        name VARCHAR(255),
        date DATE,
        event_type VARCHAR(100),
        description TEXT,
        venue_name VARCHAR(255),
        venue_address VARCHAR(255),
        venue_city VARCHAR(100),
        venue_state VARCHAR(50),
        venue_zip VARCHAR(20),
        venue_parking VARCHAR(100),
        venue_website VARCHAR(255),
        registration_url VARCHAR(255),
        sponsorship_url VARCHAR(255),
        sponsorship_tiers TEXT,
        sponsorship_contact VARCHAR(255),
        past_sponsors TEXT,
        dress_code VARCHAR(100),
        organizer_id INT,
        UNIQUE (uid),
        FOREIGN KEY (organizer_id) REFERENCES organizers(id)
          ON UPDATE CASCADE ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    raw_links_table = """
    CREATE TABLE IF NOT EXISTS raw_links (
        id INT AUTO_INCREMENT PRIMARY KEY,
        query VARCHAR(255),
        title VARCHAR(255),
        url VARCHAR(500) UNIQUE,
        snippet TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    parse_logs_table = """
    CREATE TABLE IF NOT EXISTS parse_logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        raw_link_id INT,
        status ENUM('success','failed') DEFAULT 'success',
        message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (raw_link_id) REFERENCES raw_links(id)
          ON UPDATE CASCADE ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    ein_logs_table = """
    CREATE TABLE IF NOT EXISTS ein_enrichment_logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        organizer_id INT,
        source VARCHAR(50),
        ein VARCHAR(20),
        status ENUM('success','failed') DEFAULT 'failed',
        message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (organizer_id) REFERENCES organizers(id)
          ON UPDATE CASCADE ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    try:
        cursor.execute(organizers_table)
        cursor.execute(events_table)
        cursor.execute(raw_links_table)
        cursor.execute(parse_logs_table)
        cursor.execute(ein_logs_table)
        conn.commit()
        print("✅ Tables created/verified.")
    except Error as e:
        print(f"❌ Error creating tables: {e}")
    finally:
        cursor.close()
        conn.close()


# =========================
# Helpers
# =========================
def generate_uid(*args):
    """Generate a stable UID hash from key fields."""
    raw = "-".join([str(a).lower().strip() for a in args if a])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:64]


# =========================
# Inserts (dedupe via uid)
# =========================
def insert_organizer(data):
    """Insert or get organizer with UID deduplication."""
    conn = create_connection()
    if not conn:
        return None
    cursor = conn.cursor()

    uid = generate_uid(data.get("organizer_name"), data.get("organizer_website"))

    sql = """
    INSERT INTO organizers 
    (uid, name, ein, website, email, phone, contact_name, contact_title, contact_email, facebook, instagram)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)
    """
    values = (
        uid, data.get("organizer_name"), data.get("organizer_ein"), data.get("organizer_website"),
        data.get("organizer_email"), data.get("organizer_phone"), data.get("organizer_contact_name"),
        data.get("organizer_contact_title"), data.get("organizer_contact_email"),
        data.get("organizer_facebook"), data.get("organizer_instagram")
    )

    try:
        cursor.execute(sql, values)
        conn.commit()
        organizer_id = cursor.lastrowid
        print(f"✅ Organizer saved (UID {uid[:8]}...): {data.get('organizer_name')}")
    except Error as e:
        print(f"❌ Error inserting organizer: {e}")
        organizer_id = None
    finally:
        cursor.close()
        conn.close()

    return organizer_id


def insert_event(data, organizer_id=None):
    """Insert event into the database with UID deduplication."""
    conn = create_connection()
    if not conn:
        return
    cursor = conn.cursor()

    # Normalize blank date to None (DATE accepts NULL)
    event_date = data.get("event_date")
    if not event_date or str(event_date).strip() == "":
        event_date = None

    uid = generate_uid(data.get("event_name"), event_date, data.get("venue_name"))

    sql = """
    INSERT INTO events 
    (uid, name, date, event_type, description, venue_name, venue_address, venue_city, venue_state, venue_zip,
     venue_parking, venue_website, registration_url, sponsorship_url, sponsorship_tiers, sponsorship_contact,
     past_sponsors, dress_code, organizer_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE id=id
    """
    values = (
        uid, data.get("event_name"), event_date, data.get("event_type"), data.get("description"),
        data.get("venue_name"), data.get("venue_address"), data.get("venue_city"), data.get("venue_state"),
        data.get("venue_zip"), data.get("venue_parking"), data.get("venue_website"),
        data.get("registration_url"), data.get("sponsorship_url"), data.get("sponsorship_tiers"),
        data.get("sponsorship_contact"), data.get("past_sponsors"), data.get("dress_code"), organizer_id
    )

    try:
        cursor.execute(sql, values)
        conn.commit()
        print(f"✅ Event saved (UID {uid[:8]}...): {data.get('event_name')}")
    except Error as e:
        print(f"❌ Error inserting event: {e}")
    finally:
        cursor.close()
        conn.close()


# =========================
# Logs
# =========================
def log_parse_result(raw_link_id, status="success", message=None):
    conn = create_connection()
    if not conn:
        return
    cursor = conn.cursor()
    sql = "INSERT INTO parse_logs (raw_link_id, status, message) VALUES (%s, %s, %s)"
    try:
        cursor.execute(sql, (raw_link_id, status, message))
        conn.commit()
    except Error as e:
        print(f"❌ Error logging parse result: {e}")
    finally:
        cursor.close(); conn.close()

def log_ein_result(organizer_id, source, ein=None, status="failed", message=None):
    conn = create_connection()
    if not conn:
        return
    cursor = conn.cursor()
    sql = "INSERT INTO ein_enrichment_logs (organizer_id, source, ein, status, message) VALUES (%s, %s, %s, %s, %s)"
    try:
        cursor.execute(sql, (organizer_id, source, ein, status, message))
        conn.commit()
        print(f"📝 Logged EIN result for organizer {organizer_id}: {status} ({source})")
    except Error as e:
        print(f"❌ Error logging EIN result: {e}")
    finally:
        cursor.close(); conn.close()
        
def update_organizer_ein(org_id, ein):
    """Update organizer record with EIN."""
    conn = create_connection()
    if not conn:
        return
    cur = conn.cursor()
    try:
        cur.execute("UPDATE organizers SET ein = %s WHERE id = %s", (ein, org_id))
        conn.commit()
        print(f"✅ Updated organizer {org_id} with EIN {ein}")
    except Exception as e:
        print(f"❌ Error updating EIN for organizer {org_id}: {e}")
    finally:
        cur.close()
        conn.close()

def fetch_all_events():
    """Fetch all events from the database."""
    conn = create_connection()
    if not conn:
        return []
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM events")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def fetch_all_organizers():
    """Fetch all organizers from the database."""
    conn = create_connection()
    if not conn:
        return []
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM organizers")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


if __name__ == "__main__":
    # Running this file directly will ensure DB + tables exist
    ensure_database()
    create_tables()
