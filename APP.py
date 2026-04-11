import streamlit as st
import sqlite3
import pandas as pd
import math
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import io
from functools import lru_cache

# ==========================================
# CONFIG & CREDENTIALS
# ==========================================
ADMIN_USER = "barstock_tatapower"
ADMIN_PASS = "amittpddl"

CRITICAL_EXPIRY_DAYS = 7
WARNING_EXPIRY_DAYS = 30
SAFE_EXPIRY_DAYS = 90

# ==========================================
# DATABASE SETUP & HELPERS
# ==========================================

DB_NAME = "liquor_inventory.db"

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    conn = get_connection()
    c = conn.cursor()
    
    # Locations Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS LOCATIONS_TABLE (
            location_id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_name TEXT UNIQUE
        )
    ''')
    
    c.execute("INSERT OR IGNORE INTO LOCATIONS_TABLE (location_name) VALUES ('Cenpeid Guest House')")
    c.execute("INSERT OR IGNORE INTO LOCATIONS_TABLE (location_name) VALUES ('Civil Lines Guest House')")

    # Stock Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS STOCK_TABLE (
            stock_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_added TEXT,
            brand_name TEXT,
            item_name TEXT,
            ml_per_bottle REAL,
            quantity_added INTEGER,
            open_bottles INTEGER,
            closed_bottles INTEGER,
            open_ml REAL,
            total_ml_available REAL,
            bill_no TEXT,
            price REAL,
            supplier TEXT,
            remarks TEXT,
            location TEXT,
            mfg_date TEXT,
            expiry_date TEXT,
            batch_number TEXT,
            bottle_type TEXT,
            is_opened INTEGER DEFAULT 0,
            original_ml REAL,
            days_in_stock INTEGER
        )
    ''')
    
    # Event Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS EVENT_TABLE (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            occasion TEXT,
            brand_name TEXT,
            total_bottles_before INTEGER,
            total_ml_before REAL,
            ml_consumed REAL,
            closed_bottles_opened INTEGER,
            open_ml_used REAL,
            total_bottles_after INTEGER,
            total_ml_after REAL,
            open_bottles_after INTEGER,
            closed_bottles_after INTEGER,
            permit_number TEXT,
            location TEXT,
            fifo_note TEXT,
            event_notes TEXT
        )
    ''')
    
    # Brand Master Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS BRAND_MASTER (
            brand_name TEXT PRIMARY KEY,
            standard_ml REAL,
            category TEXT,
            typical_shelf_life_days INTEGER
        )
    ''')

    # Pending Stock Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS PENDING_STOCK_TABLE (
            request_id INTEGER PRIMARY KEY AUTOINCREMENT,
            item TEXT,
            brand_name TEXT,
            bottle_count INTEGER,
            ml_per_bottle REAL,
            price REAL,
            supplier TEXT,
            remarks TEXT,
            status TEXT,
            requested_by TEXT,
            date TEXT,
            location TEXT,
            mfg_date TEXT,
            expiry_date TEXT,
            created_at TEXT,
            approval_notes TEXT,
            bottle_type TEXT,
            is_opened INTEGER DEFAULT 0,
            original_ml REAL
        )
    ''')
    
    # Safe migrations
    migrations = [
        "ALTER TABLE STOCK_TABLE ADD COLUMN mfg_date TEXT",
        "ALTER TABLE STOCK_TABLE ADD COLUMN expiry_date TEXT",
        "ALTER TABLE STOCK_TABLE ADD COLUMN batch_number TEXT",
        "ALTER TABLE STOCK_TABLE ADD COLUMN bottle_type TEXT",
        "ALTER TABLE STOCK_TABLE ADD COLUMN is_opened INTEGER DEFAULT 0",
        "ALTER TABLE STOCK_TABLE ADD COLUMN original_ml REAL",
        "ALTER TABLE STOCK_TABLE ADD COLUMN days_in_stock INTEGER",
        "ALTER TABLE EVENT_TABLE ADD COLUMN fifo_note TEXT",
        "ALTER TABLE EVENT_TABLE ADD COLUMN event_notes TEXT",
        "ALTER TABLE EVENT_TABLE ADD COLUMN permit_number TEXT",
        "ALTER TABLE EVENT_TABLE ADD COLUMN location TEXT",
        "ALTER TABLE PENDING_STOCK_TABLE ADD COLUMN created_at TEXT",
        "ALTER TABLE PENDING_STOCK_TABLE ADD COLUMN approval_notes TEXT",
        "ALTER TABLE PENDING_STOCK_TABLE ADD COLUMN mfg_date TEXT",
        "ALTER TABLE PENDING_STOCK_TABLE ADD COLUMN expiry_date TEXT",
        "ALTER TABLE PENDING_STOCK_TABLE ADD COLUMN bottle_type TEXT",
        "ALTER TABLE PENDING_STOCK_TABLE ADD COLUMN is_opened INTEGER DEFAULT 0",
        "ALTER TABLE PENDING_STOCK_TABLE ADD COLUMN original_ml REAL",
        "ALTER TABLE STOCK_TABLE ADD COLUMN location TEXT",
        "ALTER TABLE BRAND_MASTER ADD COLUMN typical_shelf_life_days INTEGER",
    ]
    
    for migration in migrations:
        try:
            c.execute(migration)
        except sqlite3.OperationalError:
            pass
    
    # Create indexes
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_stock_brand_location ON STOCK_TABLE(brand_name, location)",
        "CREATE INDEX IF NOT EXISTS idx_stock_expiry ON STOCK_TABLE(expiry_date)",
        "CREATE INDEX IF NOT EXISTS idx_stock_location ON STOCK_TABLE(location)",
        "CREATE INDEX IF NOT EXISTS idx_event_brand ON EVENT_TABLE(brand_name)",
        "CREATE INDEX IF NOT EXISTS idx_event_date ON EVENT_TABLE(date)",
        "CREATE INDEX IF NOT EXISTS idx_event_location ON EVENT_TABLE(location)",
    ]
    
    for index in indexes:
        try:
            c.execute(index)
        except:
            pass

    # Cleanup null locations
    c.execute("UPDATE STOCK_TABLE SET location = 'Cenpeid Guest House' WHERE location IS NULL OR location = ''")
    c.execute("UPDATE EVENT_TABLE SET location = 'Cenpeid Guest House' WHERE location IS NULL OR location = ''")
    c.execute("UPDATE PENDING_STOCK_TABLE SET location = 'Cenpeid Guest House' WHERE location IS NULL OR location = ''")
        
    conn.commit()
    conn.close()

def run_query(query, params=()):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(query, params)
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Database Error: {e}")
        return False
    finally:
        conn.close()

def fetch_data(query, params=()):
    conn = get_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

@lru_cache(maxsize=32)
def get_active_locations():
    df = fetch_data("SELECT location_name FROM LOCATIONS_TABLE ORDER BY location_id ASC")
    if df.empty:
        return ("Cenpeid Guest House",)
    return tuple(df['location_name'].tolist())

def get_template_excel(template_type):
    buffer = io.BytesIO()
    if template_type == 'stock':
        df = pd.DataFrame(columns=[
            'item', 'brand_name', 'location', 'ml_per_bottle', 'bottle_count', 
            'batch_number', 'bottle_type', 'original_ml',
            'mfg_date', 'expiry_date', 'bill_no', 'date', 'price', 'supplier', 'remarks'
        ])
    elif template_type == 'event':
        df = pd.DataFrame(columns=[
            'date', 'occasion', 'brand_name', 'location', 'bottles_consumed', 'extra_ml', 'permit_number'
        ])
    
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Template')
    return buffer.getvalue()

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

def calculate_days_to_expiry(expiry_date_str):
    """Calculate days remaining until expiry"""
    if not expiry_date_str:
        return None
    try:
        expiry = datetime.strptime(expiry_date_str, "%Y-%m-%d")
        today = datetime.today()
        days = (expiry - today).days
        return days
    except:
        return None

def calculate_days_in_stock(date_added_str):
    """Calculate how long stock has been with us"""
    if not date_added_str:
        return None
    try:
        added = datetime.strptime(date_added_str, "%Y-%m-%d")
        today = datetime.today()
        days = (today - added).days
        return days
    except:
        return None

def get_expiry_status(days_to_expiry):
    """Return status badge and color"""
    if days_to_expiry is None:
        return "❓ Unknown", "gray"
    elif days_to_expiry < 0:
        return "⚫ EXPIRED", "red"
    elif days_to_expiry < CRITICAL_EXPIRY_DAYS:
        return "🔴 CRITICAL", "red"
    elif days_to_expiry < WARNING_EXPIRY_DAYS:
        return "🟡 EXPIRING SOON", "orange"
    elif days_to_expiry >= SAFE_EXPIRY_DAYS:
        return "🟢 GOOD", "green"
    else:
        return "🟡 EXPIRING SOON", "orange"

def get_fifo_order(brand, location):
    """Get stock ordered by FIFO (expiry date first)"""
    query = '''
        SELECT * FROM STOCK_TABLE 
        WHERE brand_name = ? AND location = ? AND total_ml_available > 0
        ORDER BY 
            CASE 
                WHEN expiry_date IS NULL THEN 1
                ELSE 0
            END,
            expiry_date ASC,
            date_added ASC
    '''
    return fetch_data(query, (brand, location))

def validate_dates(mfg_date, expiry_date):
    """Validate manufacturing and expiry dates"""
    errors = []
    warnings = []
    
    try:
        mfg = datetime.strptime(mfg_date, "%Y-%m-%d") if mfg_date else None
        exp = datetime.strptime(expiry_date, "%Y-%m-%d") if expiry_date else None
        today = datetime.today()
        
        if mfg and mfg > today:
            errors.append("❌ Manufacturing date cannot be in the future.")
        
        if mfg and exp and exp <= mfg:
            errors.append("❌ Expiry date must be after manufacturing date.")
        
        if exp and exp < today:
            errors.append("⚫ Stock is already EXPIRED!")
            
    except ValueError:
        errors.append("❌ Invalid date format. Use YYYY-MM-DD.")
    
    return errors, warnings

def rename_for_display(df):
    """Rename columns for better display"""
    mapping = {
        'item_name': 'Item',
        'quantity_added': 'Qty',
        'open_bottles': 'Open Bottles',
        'closed_bottles': 'Sealed Bottles',
        'total_ml_available': 'Total ML',
        'date_added': 'Date Added',
        'brand_name': 'Brand',
        'ml_per_bottle': 'ML/Bottle',
        'bill_no': 'Bill No',
        'price': 'Price',
        'supplier': 'Supplier',
        'remarks': 'Remarks',
        'mfg_date': 'MFG Date',
        'expiry_date': 'Expiry',
        'batch_number': 'Batch',
        'bottle_type': 'Type',
        'is_opened': 'Opened',
        'original_ml': 'Original ML',
        'days_to_expiry': 'Days Left',
        'days_in_stock': 'Days in Stock',
        'cost_per_ml': 'Cost/ML',
        'stock_level': 'Level',
        'expiry_status': 'Status',
        'event_id': 'Event ID',
        'date': 'Date',
        'occasion': 'Occasion',
        'total_bottles_before': 'Before',
        'total_ml_before': 'ML Before',
        'ml_consumed': 'ML Used',
        'closed_bottles_opened': 'Opened',
        'open_ml_used': 'Open ML Used',
        'total_bottles_after': 'After',
        'total_ml_after': 'ML After',
        'open_bottles_after': 'Open After',
        'closed_bottles_after': 'Sealed After',
        'permit_number': 'Permit',
        'item': 'Item',
        'bottle_count': 'Count',
        'status': 'Status',
        'requested_by': 'By',
        'location': 'Location',
        'fifo_note': 'FIFO Note',
        'event_notes': 'Notes',
    }
    return df.rename(columns=mapping)

def export_to_excel(df, sheet_name="Data"):
    """Enhanced Excel export with formatting"""
    buffer = io.BytesIO()
    
    try:
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1,
                'align': 'center'
            })
            
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            for i, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).apply(len).max(), len(col)) + 2
                worksheet.set_column(i, i, min(max_len, 50))
        
        buffer.seek(0)
        return buffer.getvalue()
    except Exception as e:
        st.error(f"Error exporting: {e}")
        return None

# ==========================================
# CORE CONSUMPTION LOGIC
# ==========================================

def apply_consumption_logic(brand, location, ml_consumed):
    """Apply FIFO consumption logic"""
    if ml_consumed <= 0:
        return False, "Consumption amount must be greater than zero."

    conn = get_connection()
    c = conn.cursor()
    
    try:
        stock_rows = get_fifo_order(brand, location)
        
        if stock_rows.empty:
            raise ValueError(f"No stock available for {brand} at {location}.")
        
        # Check for expired stock
        expired_rows = stock_rows[
            stock_rows['expiry_date'].apply(lambda x: calculate_days_to_expiry(x) is not None and calculate_days_to_expiry(x) < 0)
        ]
        if not expired_rows.empty:
            st.warning(f"⚠️ WARNING: {len(expired_rows)} expired batch(es) detected. Remove them!")
            
        total_ml_before = float(stock_rows['total_ml_available'].sum())
        total_closed_before = int(stock_rows['closed_bottles'].sum())
        total_open_before = int(stock_rows['open_bottles'].sum())
        total_bottles_before = total_closed_before + total_open_before
        
        if ml_consumed > total_ml_before:
            raise ValueError(f"Insufficient stock. Requested: {ml_consumed:,.0f} ML, Available: {total_ml_before:,.0f} ML")
            
        remaining_to_consume = float(ml_consumed)
        total_open_ml_used = 0.0
        total_closed_opened = 0
        fifo_details = []
        
        for index, row in stock_rows.iterrows():
            if remaining_to_consume <= 0:
                break
                
            s_id = row['stock_id']
            r_open_ml = float(row['open_ml'])
            r_closed = int(row['closed_bottles'])
            r_open = int(row['open_bottles'])
            r_ml_per_bottle = float(row['ml_per_bottle'])
            r_total_ml = float(row['total_ml_available'])
            batch_info = f"Batch: {row['batch_number'] if row['batch_number'] else 'N/A'} | Expiry: {row['expiry_date']}"
            bottle_type = "Opened" if row['is_opened'] == 1 else "Sealed"
            
            consumed_from_row = min(remaining_to_consume, r_total_ml)
            fifo_details.append(f"{bottle_type} - {batch_info}: -{consumed_from_row:.0f}ML")
            
            if r_open_ml >= consumed_from_row:
                r_open_ml -= consumed_from_row
                total_open_ml_used += consumed_from_row
                remaining_to_consume -= consumed_from_row
            else:
                consumed_from_open = r_open_ml
                total_open_ml_used += consumed_from_open
                remaining_in_row = consumed_from_row - consumed_from_open
                r_open_ml = 0.0
                
                bottles_to_open = math.ceil(remaining_in_row / r_ml_per_bottle)
                bottles_to_open = min(bottles_to_open, r_closed)
                
                r_closed -= bottles_to_open
                r_open += bottles_to_open
                total_closed_opened += bottles_to_open
                
                r_open_ml = (bottles_to_open * r_ml_per_bottle) - remaining_in_row
                remaining_to_consume -= consumed_from_row
                
            r_total_ml = (r_closed * r_ml_per_bottle) + r_open_ml
            if r_total_ml == 0:
                r_open = 0 
                
            c.execute('''
                UPDATE STOCK_TABLE 
                SET open_ml = ?, closed_bottles = ?, open_bottles = ?, total_ml_available = ?
                WHERE stock_id = ?
            ''', (r_open_ml, r_closed, r_open, r_total_ml, s_id))
            
        conn.commit()
        
        after_df = pd.read_sql_query(
            "SELECT SUM(closed_bottles) as c, SUM(open_bottles) as o FROM STOCK_TABLE WHERE brand_name = ? AND location = ?", 
            conn, params=(brand, location)
        )
        total_closed_after = int(after_df.iloc[0]['c']) if pd.notna(after_df.iloc[0]['c']) else 0
        total_open_after = int(after_df.iloc[0]['o']) if pd.notna(after_df.iloc[0]['o']) else 0
        total_bottles_after = total_closed_after + total_open_after
        total_ml_after = total_ml_before - ml_consumed
        
        stats = {
            'total_ml_before': total_ml_before,
            'total_bottles_before': total_bottles_before,
            'total_ml_after': total_ml_after,
            'total_bottles_after': total_bottles_after,
            'open_bottles_after': total_open_after,
            'closed_bottles_after': total_closed_after,
            'open_ml_used': total_open_ml_used,
            'closed_bottles_opened': total_closed_opened,
            'fifo_note': " | ".join(fifo_details)
        }
        
        return True, stats
        
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

# ==========================================
# PAGE FUNCTIONS
# ==========================================

def dashboard():
    """Enhanced Dashboard with comprehensive metrics"""
    st.title("📊 Dashboard & Inventory Overview")
    
    locations = list(get_active_locations())
    stock_df = fetch_data("SELECT * FROM STOCK_TABLE")
    events_df = fetch_data("SELECT * FROM EVENT_TABLE")
    
    if stock_df.empty:
        st.info("📭 No stock data available yet. Start by adding stock!")
        return
    
    # Calculate metrics
    stock_df['days_to_expiry'] = stock_df['expiry_date'].apply(calculate_days_to_expiry)
    stock_df['days_in_stock'] = stock_df['date_added'].apply(calculate_days_in_stock)
    stock_df['cost_per_ml'] = (stock_df['price'] / stock_df['total_ml_available']).replace([float('inf'), -float('inf')], 0)
    
    expired = stock_df[stock_df['days_to_expiry'] < 0]
    expiring_critical = stock_df[(stock_df['days_to_expiry'] >= 0) & (stock_df['days_to_expiry'] < CRITICAL_EXPIRY_DAYS)]
    expiring_warning = stock_df[(stock_df['days_to_expiry'] >= CRITICAL_EXPIRY_DAYS) & (stock_df['days_to_expiry'] < WARNING_EXPIRY_DAYS)]
    good_stock = stock_df[stock_df['days_to_expiry'] >= WARNING_EXPIRY_DAYS]
    
    # Alert cards
    st.markdown("### 🚨 PRIORITY ALERTS")
    alert_col1, alert_col2, alert_col3, alert_col4 = st.columns(4)
    
    exp_bottles = len(expired)
    exp_ml = expired['total_ml_available'].sum() if not expired.empty else 0
    alert_col1.metric("⚫ EXPIRED", f"{exp_bottles} batches", f"{exp_ml:,.0f} ML", delta_color="inverse")
    
    crit_bottles = len(expiring_critical)
    crit_ml = expiring_critical['total_ml_available'].sum() if not expiring_critical.empty else 0
    alert_col2.metric("🔴 CRITICAL", f"{crit_bottles} batches", f"{crit_ml:,.0f} ML", delta_color="inverse")
    
    warn_bottles = len(expiring_warning)
    warn_ml = expiring_warning['total_ml_available'].sum() if not expiring_warning.empty else 0
    alert_col3.metric("🟡 WARNING", f"{warn_bottles} batches", f"{warn_ml:,.0f} ML")
    
    good_bottles = len(good_stock)
    good_ml = good_stock['total_ml_available'].sum() if not good_stock.empty else 0
    alert_col4.metric("🟢 GOOD", f"{good_bottles} batches", f"{good_ml:,.0f} ML")
    
    if not expired.empty:
        st.error("⚠️ **ACTION REQUIRED**: Following stock has EXPIRED:")
        exp_display = rename_for_display(expired[['brand_name', 'location', 'expiry_date', 'bottle_type', 'total_ml_available']])
        st.dataframe(exp_display, use_container_width=True)
    
    st.markdown("---")
    
    # Next 14 days
    st.markdown("### 📅 Next 14 Days Preview")
    next_14_days = stock_df[
        (stock_df['days_to_expiry'] >= 0) & 
        (stock_df['days_to_expiry'] <= 14)
    ].sort_values('expiry_date')
    
    if not next_14_days.empty:
        preview_display = rename_for_display(next_14_days[['brand_name', 'location', 'expiry_date', 'days_to_expiry', 'bottle_type', 'closed_bottles', 'open_bottles', 'total_ml_available']])
        st.dataframe(preview_display, use_container_width=True)
    else:
        st.success("✅ No stock expiring in next 14 days")
    
    st.markdown("---")
    
    # Main metrics
    st.markdown("### 🌍 Overall Inventory Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    valid_ml = stock_df[stock_df['days_to_expiry'] >= 0]['total_ml_available'].sum()
    valid_bottles = (stock_df[stock_df['days_to_expiry'] >= 0]['open_bottles'].sum() + 
                     stock_df[stock_df['days_to_expiry'] >= 0]['closed_bottles'].sum())
    
    sealed_count = len(stock_df[stock_df['is_opened'] == 0])
    opened_count = len(stock_df[stock_df['is_opened'] == 1])
    
    col1.metric("💧 Valid ML", f"{valid_ml:,.0f}")
    col2.metric("📦 Valid Bottles", f"{valid_bottles}")
    col3.metric("🔒 Sealed", f"{sealed_count}")
    col4.metric("🔓 Opened", f"{opened_count}")
    
    total_value = stock_df[stock_df['days_to_expiry'] >= 0]['price'].sum()
    col5.metric("💰 Total Value", f"₹{total_value:,.0f}")
    
    st.markdown("---")
    
    # Location-wise breakdown
    st.markdown("### 🏢 Location-wise Breakdown")
    
    for loc in locations:
        with st.expander(f"📌 {loc}", expanded=True):
            loc_stock = stock_df[stock_df['location'] == loc].copy()
            loc_stock['days_to_expiry'] = loc_stock['expiry_date'].apply(calculate_days_to_expiry)
            
            if loc_stock.empty:
                st.info(f"No stock data for {loc}.")
                continue
            
            loc_ml = loc_stock[loc_stock['days_to_expiry'] >= 0]['total_ml_available'].sum()
            loc_bottles = (loc_stock[loc_stock['days_to_expiry'] >= 0]['open_bottles'].sum() + 
                           loc_stock[loc_stock['days_to_expiry'] >= 0]['closed_bottles'].sum())
            loc_sealed = loc_stock[loc_stock['days_to_expiry'] >= 0]['closed_bottles'].sum()
            loc_open = loc_stock[loc_stock['days_to_expiry'] >= 0]['open_bottles'].sum()
            loc_sealed_batches = len(loc_stock[loc_stock['is_opened'] == 0])
            loc_opened_batches = len(loc_stock[loc_stock['is_opened'] == 1])
            
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Total ML", f"{loc_ml:,.0f}")
            c2.metric("Bottles", f"{loc_bottles}")
            c3.metric("Sealed", f"{loc_sealed}")
            c4.metric("Open", f"{loc_open}")
            c5.metric("🔒 Batches", f"{loc_sealed_batches}")
            c6.metric("🔓 Batches", f"{loc_opened_batches}")
            
            # Charts
            colA, colB = st.columns(2)
            
            with colA:
                brand_stock = loc_stock[loc_stock['days_to_expiry'] >= 0].groupby('brand_name')['total_ml_available'].sum().reset_index()
                if not brand_stock.empty:
                    fig_pie = px.pie(brand_stock, names='brand_name', values='total_ml_available', 
                                   hole=0.3, title="Stock by Brand")
                    st.plotly_chart(fig_pie, use_container_width=True)
            
            with colB:
                loc_events = events_df[events_df['location'] == loc] if not events_df.empty else pd.DataFrame()
                if not loc_events.empty:
                    brand_cons = loc_events.groupby('brand_name')['ml_consumed'].sum().reset_index()
                    fig_bar = px.bar(brand_cons, x='brand_name', y='ml_consumed', text_auto=True, 
                                   title="Consumption by Brand")
                    st.plotly_chart(fig_bar, use_container_width=True)
                else:
                    st.info("No consumption events yet")

def add_stock():
    """Add Stock with Sealed/Opened Toggle"""
    st.title("➕ Add Stock (With Opened Bottle Support)")
    st.info("✅ **Support for Both**: Sealed bottles (full) OR Opened bottles (partial quantities)")
    
    locations = list(get_active_locations())
    
    col_loc, col_date = st.columns(2)
    with col_loc:
        location = st.selectbox("Location *", locations)
    with col_date:
        date_added = st.date_input("Date Added", datetime.today())
    
    # ========== BOTTLE TYPE TOGGLE ==========
    st.markdown("### 🍾 Bottle Type Selection")
    
    bottle_type_col1, bottle_type_col2 = st.columns(2)
    
    with bottle_type_col1:
        is_sealed = st.radio(
            "Select Bottle Type",
            ["🔒 Sealed (Full Bottles)", "🔓 Opened (Partial Quantity)"],
            horizontal=False
        )
    
    with bottle_type_col2:
        if "Sealed" in is_sealed:
            st.success(
                "✅ **Sealed Bottles**\n\n"
                "• Full bottles with standard size\n"
                "• Quantity = Number of bottles\n"
                "• Total ML = Qty × ML per Bottle\n"
                "• Example: 5 × 750ML = 3750ML"
            )
        else:
            st.info(
                "ℹ️ **Opened Bottles**\n\n"
                "• Partially consumed bottles\n"
                "• Enter exact ML remaining\n"
                "• Quantity = 1 (for tracking)\n"
                "• Example: 450ML of old stock"
            )
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        item_options = ["Whiskey", "Vodka", "Beer", "Rum", "Wine", "Gin", "Tequila", "Brandy", "Champagne", "Others"]
        selected_item = st.selectbox("Item *", item_options)
        
        if selected_item == "Others":
            actual_item = st.text_input("Specify Item *")
        else:
            actual_item = selected_item
            
        brand_name = st.text_input("Brand Name *")
        
    with col2:
        supplier = st.text_input("Supplier")
        batch_number = st.text_input("Batch/Lot Number (Optional)", help="e.g., BK2024001")
        price = st.number_input("Price per Bottle (₹)", min_value=0.0, step=10.0)
    
    remarks = st.text_area("Remarks/Notes", placeholder="Any special notes about this stock...")
    
    # ========== CONDITIONAL FIELDS ==========
    
    if "Sealed" in is_sealed:
        st.markdown("### 📦 Sealed Bottles Details")
        
        col_qty, col_ml = st.columns(2)
        
        with col_qty:
            bottle_count = st.number_input("Number of Sealed Bottles *", min_value=1, value=1, step=1)
            if bottle_count > 100:
                st.warning(f"⚠️ Large quantity: {bottle_count} bottles")
        
        with col_ml:
            ml_per_bottle = st.number_input("ML per Bottle *", min_value=1.0, value=750.0, step=50.0)
        
        total_ml_display = bottle_count * ml_per_bottle
        open_bottles_count = 0
        closed_bottles_count = bottle_count
        open_ml_amount = 0.0
        quantity_for_db = bottle_count
        is_opened_flag = 0
        original_ml_stored = None
        
    else:  # OPENED
        st.markdown("### 🍷 Opened Bottle Details")
        
        col_ml_std, col_ml_actual = st.columns(2)
        
        with col_ml_std:
            ml_per_bottle = st.number_input("Standard Bottle Size (ML) *", min_value=1.0, value=750.0, step=50.0)
            st.caption("This is the original bottle size (for reference)")
        
        with col_ml_actual:
            open_ml_amount = st.number_input("Current ML in Bottle *", min_value=0.1, value=500.0, step=10.0)
            st.caption("Actual ML remaining in the opened bottle")
        
        if open_ml_amount >= ml_per_bottle:
            st.error(f"❌ Current ML ({open_ml_amount}) cannot be >= Standard size ({ml_per_bottle})")
            st.stop()
        
        total_ml_display = open_ml_amount
        bottle_count = 1
        open_bottles_count = 1
        closed_bottles_count = 0
        quantity_for_db = 1
        is_opened_flag = 1
        original_ml_stored = ml_per_bottle
        
        st.info(f"📊 This opened bottle contains **{open_ml_amount} ML** out of original **{ml_per_bottle} ML**")
    
    # ========== DATE FIELDS ==========
    st.markdown("### 📅 Manufacturing & Expiry Dates")
    col_mfg, col_exp = st.columns(2)
    
    with col_mfg:
        mfg_date = st.date_input("Manufacturing Date *", value=None, format="YYYY-MM-DD")
    with col_exp:
        exp_date = st.date_input("Expiry Date *", value=None, format="YYYY-MM-DD")
    
    if mfg_date and exp_date:
        errors, warnings = validate_dates(mfg_date.strftime("%Y-%m-%d"), exp_date.strftime("%Y-%m-%d"))
        
        for err in errors:
            st.error(err)
        
        if not errors:
            days_to_exp = calculate_days_to_expiry(exp_date.strftime("%Y-%m-%d"))
            status, color = get_expiry_status(days_to_exp)
            
            col_status, col_shelf, col_cost = st.columns(3)
            col_status.metric("🔍 Status", status.split()[1] if len(status.split()) > 1 else status)
            col_shelf.metric("📅 Days Left", f"{days_to_exp} days")
            col_cost.metric("💵 Cost/ML", f"₹{price/ml_per_bottle:.2f}" if ml_per_bottle > 0 else "N/A")
    
    # ========== SUMMARY ==========
    st.markdown("---")
    st.markdown("### 📋 Summary Before Saving")
    
    summary_col1, summary_col2, summary_col3 = st.columns(3)
    
    with summary_col1:
        st.metric("🍾 Type", "Sealed" if "Sealed" in is_sealed else "Opened")
    with summary_col2:
        st.metric("📦 Qty", f"{quantity_for_db}")
    with summary_col3:
        st.metric("💧 Total ML", f"{total_ml_display:,.0f}")
    
    if "Sealed" in is_sealed:
        st.info(f"✅ Adding {bottle_count} sealed bottles × {ml_per_bottle}ML = {total_ml_display:,.0f}ML total")
    else:
        st.info(f"ℹ️ Adding 1 opened bottle with {open_ml_amount}ML remaining")
    
    # ========== SAVE ==========
    if st.button("💾 Save Stock", type="primary", use_container_width=True):
        if not brand_name or not actual_item:
            st.error("❌ Item and Brand Name are required.")
        elif not mfg_date or not exp_date:
            st.error("❌ Manufacturing Date and Expiry Date are required.")
        else:
            errors, _ = validate_dates(mfg_date.strftime("%Y-%m-%d"), exp_date.strftime("%Y-%m-%d"))
            if errors:
                st.error("\n".join(errors))
            else:
                run_query("INSERT OR IGNORE INTO BRAND_MASTER (brand_name, standard_ml, category) VALUES (?, ?, ?)", 
                          (brand_name.strip(), ml_per_bottle, actual_item.strip()))
                
                query = '''
                    INSERT INTO STOCK_TABLE 
                    (date_added, brand_name, item_name, ml_per_bottle, quantity_added, open_bottles, 
                     closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks, 
                     location, mfg_date, expiry_date, batch_number, bottle_type, is_opened, original_ml, days_in_stock)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                
                bottle_type_val = "Sealed" if "Sealed" in is_sealed else "Opened"
                days_in_stock = 0
                
                success = run_query(query, (
                    date_added.strftime("%Y-%m-%d"),
                    brand_name.strip(),
                    actual_item.strip(),
                    ml_per_bottle,
                    quantity_for_db,
                    open_bottles_count,
                    closed_bottles_count,
                    open_ml_amount,
                    total_ml_display,
                    "",
                    price,
                    supplier.strip(),
                    remarks.strip(),
                    location,
                    mfg_date.strftime("%Y-%m-%d"),
                    exp_date.strftime("%Y-%m-%d"),
                    batch_number.strip(),
                    bottle_type_val,
                    is_opened_flag,
                    original_ml_stored,
                    days_in_stock
                ))
                
                if success:
                    if "Sealed" in is_sealed:
                        st.success(f"✅ Added {quantity_for_db} sealed bottles of {brand_name}")
                        st.info(f"📦 Type: Sealed | Batch: {batch_number if batch_number else 'N/A'} | Expiry: {exp_date.strftime('%Y-%m-%d')} | Location: {location}")
                    else:
                        st.success(f"✅ Added opened bottle of {brand_name}")
                        st.info(f"🍷 Type: Opened | {open_ml_amount}ML remaining | Batch: {batch_number if batch_number else 'N/A'} | Expiry: {exp_date.strftime('%Y-%m-%d')} | Location: {location}")

def view_stock():
    """View Stock with Bottle Type Indicator"""
    st.title("📦 View & Manage Stock")
    
    df = fetch_data("SELECT * FROM STOCK_TABLE ORDER BY expiry_date ASC, date_added DESC")
    locations = list(get_active_locations())
    
    if df.empty:
        st.info("📭 No stock data found.")
        return
    
    # Calculate columns
    df['days_to_expiry'] = df['expiry_date'].apply(calculate_days_to_expiry)
    df['expiry_status'] = df['days_to_expiry'].apply(lambda x: get_expiry_status(x)[0])
    df['cost_per_ml'] = (df['price'] / df['total_ml_available']).replace([float('inf'), -float('inf')], 0)
    df['bottle_type_display'] = df.apply(
        lambda row: f"🔓 Opened ({row['open_ml']:.0f}ML)" if row['is_opened'] == 1 else f"🔒 Sealed ({row['quantity_added']} bottles)",
        axis=1
    )
    
    # Summary
    st.markdown("### 📊 Quick Summary")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    valid_stock = df[df['days_to_expiry'] >= 0]
    sealed_count = len(valid_stock[valid_stock['is_opened'] == 0])
    opened_count = len(valid_stock[valid_stock['is_opened'] == 1])
    
    col1.metric("✅ Valid Batches", len(valid_stock))
    col2.metric("🔒 Sealed", sealed_count)
    col3.metric("🔓 Opened", opened_count)
    col4.metric("📦 Total ML", f"{valid_stock['total_ml_available'].sum():,.0f}")
    col5.metric("💵 Value", f"₹{valid_stock['price'].sum():,.0f}")
    
    st.markdown("---")
    
    # Filters
    st.markdown("### 🔍 Advanced Filters")
    
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
    
    with filter_col1:
        filter_loc = st.multiselect("Location", ["All"] + locations, default=["All"])
        if "All" not in filter_loc:
            df = df[df['location'].isin(filter_loc)]
    
    with filter_col2:
        brands = sorted(df['brand_name'].unique().tolist())
        filter_brand = st.multiselect("Brand", ["All"] + brands, default=["All"])
        if "All" not in filter_brand:
            df = df[df['brand_name'].isin(filter_brand)]
    
    with filter_col3:
        bottle_types = ["All", "🔒 Sealed Only", "🔓 Opened Only"]
        filter_bottle_type = st.selectbox("Bottle Type", bottle_types)
        if filter_bottle_type == "🔒 Sealed Only":
            df = df[df['is_opened'] == 0]
        elif filter_bottle_type == "🔓 Opened Only":
            df = df[df['is_opened'] == 1]
    
    with filter_col4:
        statuses = ["All", "🟢 GOOD", "🟡 EXPIRING SOON", "🔴 CRITICAL", "⚫ EXPIRED"]
        filter_status = st.selectbox("Expiry Status", statuses)
        if filter_status != "All":
            df = df[df['expiry_status'] == filter_status]
    
    st.markdown("---")
    
    # Table
    if not df.empty:
        display_cols = [
            'brand_name', 'location', 'batch_number', 'bottle_type_display', 
            'mfg_date', 'expiry_date', 'days_to_expiry', 'quantity_added', 
            'open_bottles', 'total_ml_available', 'cost_per_ml', 'supplier'
        ]
        
        df_display = rename_for_display(df[[col for col in display_cols if col in df.columns]])
        st.dataframe(df_display, use_container_width=True, height=400)
        
        st.markdown("---")
        
        # Export
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df_display.to_excel(writer, index=False, sheet_name='Stock')
        
        st.download_button(
            label="📥 Download as Excel",
            data=buffer.getvalue(),
            file_name=f"stock_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.ms-excel"
        )
    else:
        st.warning("⚠️ No data matches your filters")

def upload_stock_excel():
    """Upload Stock Excel with Opened Bottle Support"""
    st.title("📁 Bulk Upload Stock")
    
    st.download_button(
        label="📥 Download Stock Template",
        data=get_template_excel('stock'),
        file_name="stock_template.xlsx",
        mime="application/vnd.ms-excel"
    )
    
    st.info("""
    📌 **Supported Columns**: 
    - item, brand_name, location, ml_per_bottle, bottle_count
    - mfg_date, expiry_date, batch_number, date, price, supplier, remarks
    - **NEW**: bottle_type (Sealed/Opened), original_ml (for opened bottles)
    
    📌 **For Sealed**: bottle_type="Sealed", bottle_count=number
    📌 **For Opened**: bottle_type="Opened", bottle_count=1, original_ml=750
    """)
    
    uploaded_file = st.file_uploader("Choose a .xlsx file (Stock)", type=["xlsx"])
    active_locations = list(get_active_locations())
    default_loc = active_locations[0] if active_locations else "Unknown"
    
    if uploaded_file is not None:
        try:
            df = pd.read_excel(uploaded_file)
            st.write("📄 File Preview (First 5 rows):")
            st.dataframe(df.head())
            
            st.markdown("---")
            
            validation_passed = 0
            validation_failed = 0
            
            for index, row in df.iterrows():
                brand_name = str(row.get('brand_name', '')).strip()
                if not brand_name:
                    validation_failed += 1
                    continue
                
                mfg_date = str(row.get('mfg_date', ''))[:10] if pd.notna(row.get('mfg_date')) else None
                expiry_date = str(row.get('expiry_date', ''))[:10] if pd.notna(row.get('expiry_date')) else None
                
                if not mfg_date or not expiry_date:
                    validation_failed += 1
                    continue
                
                errors, _ = validate_dates(mfg_date, expiry_date)
                if errors:
                    validation_failed += 1
                else:
                    validation_passed += 1
            
            col_pass, col_fail = st.columns(2)
            col_pass.metric("✅ Valid", validation_passed)
            col_fail.metric("❌ Invalid", validation_failed)
            
            if st.button("Import Stock Data", type="primary", use_container_width=True):
                success_count = 0
                failed_count = 0
                opened_count = 0
                sealed_count = 0
                
                with st.spinner("Importing..."):
                    for index, row in df.iterrows():
                        brand_name = str(row.get('brand_name', '')).strip()
                        if not brand_name:
                            failed_count += 1
                            continue
                        
                        location = str(row.get('location', default_loc)).strip()
                        item_name = str(row.get('item', ''))
                        ml_per_bottle = float(row.get('ml_per_bottle', 750.0)) if pd.notna(row.get('ml_per_bottle')) else 750.0
                        bottle_type = str(row.get('bottle_type', 'Sealed')).strip()
                        batch_number = str(row.get('batch_number', ''))[:50] if pd.notna(row.get('batch_number')) else ''
                        mfg_date = str(row.get('mfg_date', ''))[:10] if pd.notna(row.get('mfg_date')) else None
                        expiry_date = str(row.get('expiry_date', ''))[:10] if pd.notna(row.get('expiry_date')) else None
                        date_val = str(row.get('date', datetime.today().strftime("%Y-%m-%d")))[:10]
                        price = float(row.get('price', 0.0)) if pd.notna(row.get('price')) else 0.0
                        supplier = str(row.get('supplier', ''))
                        remarks = str(row.get('remarks', ''))
                        
                        if not mfg_date or not expiry_date:
                            failed_count += 1
                            continue
                        
                        errors, _ = validate_dates(mfg_date, expiry_date)
                        if errors:
                            failed_count += 1
                            continue
                        
                        # Handle sealed vs opened
                        if bottle_type.lower() == 'opened':
                            quantity_added = 1
                            open_bottles = 1
                            closed_bottles = 0
                            open_ml = ml_per_bottle
                            total_ml = ml_per_bottle
                            is_opened = 1
                            original_ml = float(row.get('original_ml', 750.0)) if pd.notna(row.get('original_ml')) else 750.0
                            opened_count += 1
                        else:
                            quantity_added = int(row.get('bottle_count', 1)) if pd.notna(row.get('bottle_count')) else 1
                            open_bottles = 0
                            closed_bottles = quantity_added
                            open_ml = 0.0
                            total_ml = quantity_added * ml_per_bottle
                            is_opened = 0
                            original_ml = None
                            sealed_count += 1
                        
                        run_query("INSERT OR IGNORE INTO BRAND_MASTER (brand_name, standard_ml, category) VALUES (?, ?, ?)", 
                                  (brand_name, ml_per_bottle, item_name))
                        
                        query = '''
                            INSERT INTO STOCK_TABLE 
                            (date_added, brand_name, item_name, ml_per_bottle, quantity_added, open_bottles, 
                             closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks, location, 
                             mfg_date, expiry_date, batch_number, bottle_type, is_opened, original_ml, days_in_stock)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        '''
                        
                        if run_query(query, (date_val, brand_name, item_name, ml_per_bottle, quantity_added, 
                                             open_bottles, closed_bottles, open_ml, total_ml, '', price, 
                                             supplier, remarks, location, mfg_date, expiry_date, batch_number, 
                                             bottle_type, is_opened, original_ml, 0)):
                            success_count += 1
                        else:
                            failed_count += 1
                
                st.success(f"✅ Successfully imported {success_count} rows!")
                st.info(f"🔒 {sealed_count} sealed | 🔓 {opened_count} opened")
                if failed_count > 0:
                    st.warning(f"⚠️ Failed: {failed_count} rows")
                
        except Exception as e:
            st.error(f"❌ Error: {e}")

def create_event():
    """Create consumption event with FIFO"""
    st.title("🎉 Create Event (Consumption)")
    st.info("💡 **FIFO Applied**: System auto-selects expiring-soonest stock for rotation")
    
    locations = list(get_active_locations())
    location = st.selectbox("Select Location *", locations)
    
    brands_df = fetch_data("SELECT DISTINCT brand_name FROM STOCK_TABLE WHERE total_ml_available > 0 AND location = ?", (location,))
    if brands_df.empty:
        st.warning(f"📭 No available stock at {location}.")
        return
        
    brand = st.selectbox("Select Brand to Consume", brands_df['brand_name'].tolist())
    
    stock_rows = fetch_data("SELECT SUM(closed_bottles) as c, SUM(open_bottles) as o, SUM(total_ml_available) as t, MAX(ml_per_bottle) as m FROM STOCK_TABLE WHERE brand_name = ? AND location = ? AND total_ml_available > 0", (brand, location))
    
    if not stock_rows.empty and stock_rows.iloc[0]['t'] is not None:
        total_ml_before = stock_rows.iloc[0]['t']
        total_closed_before = stock_rows.iloc[0]['c']
        total_open_before = stock_rows.iloc[0]['o']
        ml_per_bottle = stock_rows.iloc[0]['m']
        total_bottles_before = total_closed_before + total_open_before
        
        st.info(f"📦 **Stock at {location}**: {total_bottles_before} Bottles | {total_ml_before:,.2f} Total ML")
        
        # Show FIFO order
        fifo_df = get_fifo_order(brand, location)
        fifo_df['days_to_expiry'] = fifo_df['expiry_date'].apply(calculate_days_to_expiry)
        fifo_df['expiry_status'] = fifo_df['days_to_expiry'].apply(lambda x: get_expiry_status(x)[0])
        
        st.markdown("#### 📋 Consumption Order (FIFO Priority):")
        fifo_cols = ['batch_number', 'bottle_type', 'mfg_date', 'expiry_date', 'days_to_expiry', 'expiry_status', 'closed_bottles', 'open_bottles', 'total_ml_available']
        fifo_display = rename_for_display(fifo_df[[col for col in fifo_cols if col in fifo_df.columns]])
        st.dataframe(fifo_display, use_container_width=True, height=300)
        
        st.markdown("---")
        
        # Event details
        col1, col2 = st.columns(2)
        with col1:
            date = st.date_input("Event Date", datetime.today())
            occasion = st.text_input("Occasion / Event Name *")
        with col2:
            permit_number = st.text_input("Permit Number (e.g., P10)")
            event_notes = st.text_area("Event Notes", placeholder="Details about this event...")
        
        st.markdown("### Consumption Details")
        auto_calc = st.toggle("Auto calculate from bottles only", value=False)
        
        col_b, col_m = st.columns(2)
        with col_b:
            bottles_consumed = st.number_input("Bottles Consumed", min_value=0, step=1, value=0)
        with col_m:
            extra_ml = st.number_input("Extra ML Consumed", min_value=0.0, step=10.0, disabled=auto_calc, value=0.0)
            if auto_calc:
                extra_ml = 0.0

        total_ml_consumed = (bottles_consumed * ml_per_bottle) + extra_ml
        
        st.markdown("---")
        st.write(f"ℹ️ *1 bottle = {ml_per_bottle} ML*")
        st.write(f"🩸 **Total ML to consume = {total_ml_consumed:,.2f} ML**")
        
        is_valid = True
        if total_ml_consumed > total_ml_before:
            st.error(f"⚠️ Consumption exceeds available stock!")
            is_valid = False
        elif extra_ml >= ml_per_bottle:
            st.error(f"⚠️ Extra ML should be less than bottle size.")
            is_valid = False
        elif total_ml_consumed == 0:
            st.warning("⚠️ Please enter consumption amount > 0.")
            is_valid = False
        elif not occasion:
            st.error("❌ Occasion name required.")
            is_valid = False
            
        if is_valid:
            preview_ml = total_ml_before - total_ml_consumed
            st.success(f"📊 **After Event**: {preview_ml:,.2f} ML left")
            
            if st.button("✅ Record Consumption", type="primary", use_container_width=True):
                success, result = apply_consumption_logic(brand, location, total_ml_consumed)
                
                if success:
                    stats = result
                    insert_q = '''
                        INSERT INTO EVENT_TABLE 
                        (date, occasion, brand_name, total_bottles_before, total_ml_before, ml_consumed, 
                         closed_bottles_opened, open_ml_used, total_bottles_after, total_ml_after, 
                         open_bottles_after, closed_bottles_after, permit_number, location, fifo_note, event_notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    run_query(insert_q, (
                        date.strftime("%Y-%m-%d"), occasion, brand, stats['total_bottles_before'], stats['total_ml_before'], 
                        total_ml_consumed, stats['closed_bottles_opened'], stats['open_ml_used'], stats['total_bottles_after'], 
                        stats['total_ml_after'], stats['open_bottles_after'], stats['closed_bottles_after'], permit_number, location,
                        stats['fifo_note'], event_notes
                    ))
                    st.success("✅ Consumption recorded!")
                    
                    with st.expander("📝 FIFO Details"):
                        st.write(stats['fifo_note'])
                else:
                    st.error(f"❌ Error: {result}")

def expiry_report():
    """Expiry & Compliance Report"""
    st.title("📊 Expiry & Compliance Report")
    st.info("Comprehensive expiry analysis with actionable insights")
    
    locations = list(get_active_locations())
    filter_loc = st.selectbox("Filter by Location", ["All Locations"] + locations)
    
    stock_df = fetch_data("SELECT * FROM STOCK_TABLE")
    
    if stock_df.empty:
        st.info("No stock data.")
        return
    
    stock_df['days_to_expiry'] = stock_df['expiry_date'].apply(calculate_days_to_expiry)
    stock_df['expiry_status'] = stock_df['days_to_expiry'].apply(lambda x: get_expiry_status(x)[0])
    stock_df['cost_per_ml'] = (stock_df['price'] / stock_df['total_ml_available']).replace([float('inf'), -float('inf')], 0)
    
    if filter_loc != "All Locations":
        stock_df = stock_df[stock_df['location'] == filter_loc]
    
    # Summary
    st.markdown("### 📈 Summary")
    
    expired_df = stock_df[stock_df['days_to_expiry'] < 0]
    critical_df = stock_df[(stock_df['days_to_expiry'] >= 0) & (stock_df['days_to_expiry'] < CRITICAL_EXPIRY_DAYS)]
    warning_df = stock_df[(stock_df['days_to_expiry'] >= CRITICAL_EXPIRY_DAYS) & (stock_df['days_to_expiry'] < WARNING_EXPIRY_DAYS)]
    good_df = stock_df[stock_df['days_to_expiry'] >= WARNING_EXPIRY_DAYS]
    
    c1, c2, c3, c4, c5 = st.columns(5)
    
    c1.metric("⚫ Expired", f"{len(expired_df)}")
    c2.metric("🔴 Critical", f"{len(critical_df)}")
    c3.metric("🟡 Warning", f"{len(warning_df)}")
    c4.metric("🟢 Good", f"{len(good_df)}")
    c5.metric("💰 Risk Value", f"₹{(expired_df['price'].sum() + critical_df['price'].sum()):,.0f}")
    
    st.markdown("---")
    
    # Expired
    st.markdown("### 🚨 ⚫ EXPIRED STOCK")
    if not expired_df.empty:
        exp_display = rename_for_display(expired_df[['batch_number', 'brand_name', 'location', 'bottle_type', 'mfg_date', 'expiry_date', 'days_to_expiry', 'closed_bottles', 'open_bottles', 'total_ml_available', 'cost_per_ml']])
        st.error(f"⚠️ {len(expired_df)} batch(es) expired. Remove immediately!")
        st.dataframe(exp_display, use_container_width=True)
    else:
        st.success("✅ No expired stock")
    
    st.markdown("---")
    
    # Critical
    st.markdown("### 🔴 CRITICAL (< 7 days)")
    if not critical_df.empty:
        crit_display = rename_for_display(critical_df[['batch_number', 'brand_name', 'location', 'bottle_type', 'expiry_date', 'days_to_expiry', 'closed_bottles', 'open_bottles', 'total_ml_available', 'cost_per_ml']])
        st.error(f"⚠️ {len(critical_df)} batch(es) expiring < 7 days!")
        st.dataframe(crit_display, use_container_width=True)
    else:
        st.success("✅ No critical expirations")
    
    st.markdown("---")
    
    # Warning
    st.markdown("### 🟡 WARNING (7-30 days)")
    if not warning_df.empty:
        warn_display = rename_for_display(warning_df[['batch_number', 'brand_name', 'location', 'bottle_type', 'expiry_date', 'days_to_expiry', 'closed_bottles', 'open_bottles', 'total_ml_available', 'cost_per_ml']])
        st.warning(f"⚠️ {len(warning_df)} batch(es) expiring 7-30 days")
        st.dataframe(warn_display, use_container_width=True)
    else:
        st.success("✅ No warnings")
    
    st.markdown("---")
    
    # Charts
    st.markdown("### 📊 Analysis")
    
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        status_counts = pd.DataFrame({
            'Status': ['Expired', 'Critical', 'Warning', 'Good'],
            'Count': [len(expired_df), len(critical_df), len(warning_df), len(good_df)]
        })
        fig_pie = px.pie(status_counts, names='Status', values='Count', title="Stock Distribution",
                        color_discrete_map={'Expired': '#ff0000', 'Critical': '#ff6b6b', 'Warning': '#ffa500', 'Good': '#00b359'})
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col_chart2:
        brand_expiry = stock_df.groupby('brand_name')['days_to_expiry'].min().reset_index().sort_values('days_to_expiry').head(10)
        fig_bar = px.bar(brand_expiry, x='brand_name', y='days_to_expiry', title="Min Days to Expiry (Top 10)")
        st.plotly_chart(fig_bar, use_container_width=True)
    
    st.markdown("---")
    
    # Export
    st.markdown("### 📥 Export")
    all_data = pd.concat([
        expired_df.assign(Category='Expired'),
        critical_df.assign(Category='Critical'),
        warning_df.assign(Category='Warning'),
        good_df.assign(Category='Good')
    ])
    
    excel_data = export_to_excel(rename_for_display(all_data[[
        'batch_number', 'brand_name', 'location', 'bottle_type', 'mfg_date', 'expiry_date', 
        'days_to_expiry', 'closed_bottles', 'open_bottles', 'total_ml_available', 'cost_per_ml'
    ]]), "Compliance")
    
    if excel_data:
        st.download_button(
            label="📥 Download Report",
            data=excel_data,
            file_name=f"expiry_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.ms-excel"
        )

def event_history():
    """Event history with filters"""
    st.title("📜 Event History")
    
    df = fetch_data("SELECT * FROM EVENT_TABLE ORDER BY date DESC")
    locations = list(get_active_locations())
    
    if df.empty:
        st.info("No events yet.")
        return
    
    df['event_date'] = pd.to_datetime(df['date'])
    
    # Filters
    st.markdown("### 🔍 Filters")
    
    col_date, col_loc, col_brand = st.columns(3)
    
    with col_date:
        date_range = st.date_input("Date Range", value=(datetime.today() - timedelta(days=30), datetime.today()), format="YYYY-MM-DD")
        if len(date_range) == 2:
            df = df[(df['event_date'] >= pd.Timestamp(date_range[0])) & (df['event_date'] <= pd.Timestamp(date_range[1]))]
    
    with col_loc:
        filter_loc = st.multiselect("Location", ["All"] + locations, default=["All"])
        if "All" not in filter_loc:
            df = df[df['location'].isin(filter_loc)]
    
    with col_brand:
        brands = sorted(df['brand_name'].unique().tolist())
        filter_brand = st.multiselect("Brand", ["All"] + brands, default=["All"])
        if "All" not in filter_brand:
            df = df[df['brand_name'].isin(filter_brand)]
    
    st.markdown("---")
    
    if not df.empty:
        st.markdown("### 📊 Summary")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Events", len(df))
        col2.metric("Total ML Used", f"{df['ml_consumed'].sum():,.0f}")
        col3.metric("Avg per Event", f"{df['ml_consumed'].mean():,.0f} ML")
        
        st.markdown("---")
        
        df_display = rename_for_display(df[['date', 'occasion', 'brand_name', 'location', 'ml_consumed', 
                                            'closed_bottles_opened', 'permit_number', 'event_notes']])
        st.dataframe(df_display, use_container_width=True, height=400)
        
        # Export
        excel_data = export_to_excel(df_display, "Events")
        if excel_data:
            st.download_button(
                label="📥 Download",
                data=excel_data,
                file_name=f"events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.ms-excel"
            )
    else:
        st.warning("No events match filters")

def brand_summary():
    """Brand Summary"""
    st.title("📋 Brand Summary")
    
    query = '''
        SELECT 
            brand_name,
            COUNT(DISTINCT batch_number) as num_batches,
            SUM(total_ml_available) as total_ml,
            SUM(CASE WHEN is_opened = 0 THEN quantity_added ELSE 0 END) as sealed_bottles,
            SUM(CASE WHEN is_opened = 1 THEN 1 ELSE 0 END) as opened_batches,
            MIN(expiry_date) as earliest_expiry,
            SUM(price) as total_value
        FROM STOCK_TABLE
        WHERE total_ml_available > 0
        GROUP BY brand_name
        ORDER BY total_ml DESC
    '''
    
    df = fetch_data(query)
    if df.empty:
        st.info("No data.")
        return
    
    df['days_to_expiry'] = df['earliest_expiry'].apply(calculate_days_to_expiry)
    df['expiry_status'] = df['days_to_expiry'].apply(lambda x: get_expiry_status(x)[0])
    
    summary_mapping = {
        'brand_name': 'Brand',
        'num_batches': 'Batches',
        'total_ml': 'Total ML',
        'sealed_bottles': 'Sealed',
        'opened_batches': 'Opened',
        'earliest_expiry': 'Earliest Exp',
        'expiry_status': 'Status',
        'total_value': 'Value'
    }
    df_display = df.rename(columns=summary_mapping)
    st.dataframe(df_display, use_container_width=True)

def manage_locations():
    """Manage Locations"""
    st.title("📍 Manage Locations")
    
    locations = list(get_active_locations())
    
    with st.form("add_loc_form", clear_on_submit=True):
        st.subheader("Add New Location")
        new_loc = st.text_input("Location Name")
        if st.form_submit_button("➕ Add", type="primary"):
            if new_loc and new_loc.strip():
                try:
                    conn = get_connection()
                    c = conn.cursor()
                    c.execute("INSERT INTO LOCATIONS_TABLE (location_name) VALUES (?)", (new_loc.strip(),))
                    conn.commit()
                    st.success(f"✅ Added: {new_loc}")
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.error("❌ Already exists")
                finally:
                    conn.close()
    
    st.markdown("---")
    st.subheader("Active Locations")
    
    for loc in locations:
        col1, col2, col3 = st.columns([2, 1, 1])
        col1.write(f"🏢 **{loc}**")
        
        stock_count = fetch_data("SELECT COUNT(*) as count FROM STOCK_TABLE WHERE location = ?", (loc,))
        col2.metric("Batches", stock_count.iloc[0]['count'])
        
        if col3.button("Delete", key=f"del_{loc}"):
            if stock_count.iloc[0]['count'] > 0:
                st.error(f"❌ Has stock - delete stock first")
            else:
                run_query("DELETE FROM LOCATIONS_TABLE WHERE location_name = ?", (loc,))
                st.success("✅ Deleted")
                st.rerun()

def approve_requests():
    """Approve Stock Requests"""
    st.title("✅ Approve Stock Requests")
    
    pending_df = fetch_data("SELECT * FROM PENDING_STOCK_TABLE WHERE status='pending' ORDER BY created_at ASC")
    
    if pending_df.empty:
        st.success("✅ No pending requests")
        return
    
    st.info(f"📨 {len(pending_df)} pending request(s)")
    
    for index, row in pending_df.iterrows():
        days_pending = (datetime.today() - datetime.strptime(row['created_at'] if row['created_at'] else row['date'], "%Y-%m-%d")).days if row['created_at'] else 0
        
        with st.expander(f"Request #{row['request_id']} | {row['location']} | {row['bottle_count']}x {row['brand_name']}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Details:**")
                st.write(f"  📍 {row['location']}")
                st.write(f"  🍾 {row['item']}")
                st.write(f"  🏷️ {row['brand_name']}")
                st.write(f"  📦 {row['bottle_count']}x {row['ml_per_bottle']}ML")
                st.write(f"  💰 ₹{row['price']}")
            
            with col2:
                st.write("**Info:**")
                st.write(f"  🏢 {row['supplier'] if row['supplier'] else 'N/A'}")
                st.write(f"  📅 MFG: {row['mfg_date'] if row['mfg_date'] else 'N/A'}")
                st.write(f"  📅 Exp: {row['expiry_date'] if row['expiry_date'] else 'N/A'}")
                st.write(f"  👤 {row['requested_by']}")
            
            col_a, col_r = st.columns(2)
            
            with col_a:
                if st.button("✅ Approve", key=f"appr_{row['request_id']}", use_container_width=True, type="primary"):
                    total_ml = row['bottle_count'] * row['ml_per_bottle']
                    
                    run_query("INSERT OR IGNORE INTO BRAND_MASTER (brand_name, standard_ml, category) VALUES (?, ?, ?)", 
                              (row['brand_name'], row['ml_per_bottle'], row['item']))
                    
                    run_query('''
                        INSERT INTO STOCK_TABLE 
                        (date_added, brand_name, item_name, ml_per_bottle, quantity_added, open_bottles, 
                         closed_bottles, open_ml, total_ml_available, price, supplier, remarks, location, 
                         mfg_date, expiry_date, batch_number, bottle_type, is_opened, original_ml, days_in_stock)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (row['date'], row['brand_name'], row['item'], row['ml_per_bottle'], 
                          row['bottle_count'], 0, row['bottle_count'], 0, total_ml, row['price'], 
                          row['supplier'], row['remarks'], row['location'], row['mfg_date'], 
                          row['expiry_date'], '', 'Sealed', 0, None, 0))
                    
                    run_query("DELETE FROM PENDING_STOCK_TABLE WHERE request_id = ?", (row['request_id'],))
                    st.success("✅ Approved!")
                    st.rerun()
            
            with col_r:
                if st.button("❌ Reject", key=f"rej_{row['request_id']}", use_container_width=True):
                    run_query("DELETE FROM PENDING_STOCK_TABLE WHERE request_id = ?", (row['request_id'],))
                    st.warning("❌ Rejected")
                    st.rerun()

# ==========================================
# MAIN APP
# ==========================================

def main():
    st.set_page_config(
        page_title="Liquor Inventory Tracker v2.2",
        page_icon="🍷",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    init_db()
    
    if 'user_role' not in st.session_state:
        st.session_state['user_role'] = None

    if st.session_state['user_role'] is None:
        st.title("🍷 Liquor Inventory System v2.2")
        st.markdown("✨ **Complete Edition with Opened Bottle Support**")
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("👤 User Access")
            if st.button("Continue as User", type="secondary", use_container_width=True):
                st.session_state['user_role'] = 'user'
                st.rerun()
                
        with col2:
            st.subheader("🛡️ Admin Login")
            admin_user = st.text_input("Username")
            admin_pass = st.text_input("Password", type="password")
            if st.button("Login", type="primary", use_container_width=True):
                if admin_user == ADMIN_USER and admin_pass == ADMIN_PASS:
                    st.session_state['user_role'] = 'admin'
                    st.rerun()
                else:
                    st.error("❌ Invalid")
        return

    st.sidebar.title("🍷 Liquor Tracker v2.2")
    st.sidebar.write(f"👤 **{st.session_state['user_role'].title()}**")
    
    if st.sidebar.button("🚪 Logout", use_container_width=True):
        st.session_state['user_role'] = None
        st.rerun()
    
    st.sidebar.markdown("---")
    
    if st.session_state['user_role'] == 'admin':
        pending = fetch_data("SELECT COUNT(*) as count FROM PENDING_STOCK_TABLE WHERE status='pending'")
        if not pending.empty and pending.iloc[0]['count'] > 0:
            st.sidebar.error(f"🔔 {pending.iloc[0]['count']} Pending")
        
        menu = [
            "📊 Dashboard",
            "📍 Manage Locations",
            "➕ Add Stock",
            "📦 View Stock",
            "📁 Upload Stock",
            "🎉 Create Event",
            "📜 Event History",
            "📊 Expiry Report",
            "📋 Brand Summary",
            "✅ Approve Requests"
        ]
    else:
        menu = [
            "📊 Dashboard",
            "📦 View Stock",
            "📜 Event History"
        ]
    
    choice = st.sidebar.radio("Navigation", menu)
    
    if choice == "📊 Dashboard":
        dashboard()
    elif choice == "📍 Manage Locations":
        manage_locations()
    elif choice == "➕ Add Stock":
        add_stock()
    elif choice == "📦 View Stock":
        view_stock()
    elif choice == "📁 Upload Stock":
        upload_stock_excel()
    elif choice == "🎉 Create Event":
        create_event()
    elif choice == "📜 Event History":
        event_history()
    elif choice == "📊 Expiry Report":
        expiry_report()
    elif choice == "📋 Brand Summary":
        brand_summary()
    elif choice == "✅ Approve Requests":
        approve_requests()

if __name__ == '__main__':
    main()
