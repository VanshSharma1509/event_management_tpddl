import streamlit as st
import sqlite3
import pandas as pd
import math
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import io

# ==========================================
# CONFIG & CREDENTIALS
# ==========================================
ADMIN_USER = "barstock_tatapower"
ADMIN_PASS = "amittpddl"

# Expiry alert thresholds (in days)
CRITICAL_EXPIRY_DAYS = 7      # Red alert
WARNING_EXPIRY_DAYS = 30      # Yellow alert
SAFE_EXPIRY_DAYS = 90         # Green status

# ==========================================
# DATABASE SETUP & HELPERS
# ==========================================

DB_NAME = "liquor_inventory.db"

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    conn = get_connection()
    c = conn.cursor()
    
    # NEW Dynamic Locations Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS LOCATIONS_TABLE (
            location_id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_name TEXT UNIQUE
        )
    ''')
    
    # Insert Default Locations ONLY if table is empty
    location_count = c.execute("SELECT COUNT(*) FROM LOCATIONS_TABLE").fetchone()[0]
    if location_count == 0:
        c.execute("INSERT INTO LOCATIONS_TABLE (location_name) VALUES ('Cenpeid Guest House')")
        c.execute("INSERT INTO LOCATIONS_TABLE (location_name) VALUES ('Civil Lines Guest House')")

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
            expiry_date TEXT
        )
    ''')
    
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
            fifo_note TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS BRAND_MASTER (
            brand_name TEXT PRIMARY KEY,
            standard_ml REAL,
            category TEXT,
            typical_shelf_life_days INTEGER
        )
    ''')

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
            expiry_date TEXT
        )
    ''')
    
    # Safe migrations for existing databases
    try: c.execute("ALTER TABLE STOCK_TABLE ADD COLUMN mfg_date TEXT")
    except sqlite3.OperationalError: pass
    
    try: c.execute("ALTER TABLE STOCK_TABLE ADD COLUMN expiry_date TEXT")
    except sqlite3.OperationalError: pass
    
    try: c.execute("ALTER TABLE EVENT_TABLE ADD COLUMN fifo_note TEXT")
    except sqlite3.OperationalError: pass
    
    try: c.execute("ALTER TABLE PENDING_STOCK_TABLE ADD COLUMN mfg_date TEXT")
    except sqlite3.OperationalError: pass
    
    try: c.execute("ALTER TABLE PENDING_STOCK_TABLE ADD COLUMN expiry_date TEXT")
    except sqlite3.OperationalError: pass
    
    try: c.execute("ALTER TABLE BRAND_MASTER ADD COLUMN typical_shelf_life_days INTEGER")
    except sqlite3.OperationalError: pass
    
    try: c.execute("ALTER TABLE EVENT_TABLE ADD COLUMN permit_number TEXT")
    except sqlite3.OperationalError: pass
    
    try: c.execute("ALTER TABLE STOCK_TABLE ADD COLUMN location TEXT")
    except sqlite3.OperationalError: pass
        
    try: c.execute("ALTER TABLE EVENT_TABLE ADD COLUMN location TEXT")
    except sqlite3.OperationalError: pass
        
    try: c.execute("ALTER TABLE PENDING_STOCK_TABLE ADD COLUMN location TEXT")
    except sqlite3.OperationalError: pass

    # Cleanup legacy null locations
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

def get_active_locations():
    df = fetch_data("SELECT location_name FROM LOCATIONS_TABLE ORDER BY location_id ASC")
    if df.empty:
        return ["Cenpeid Guest House"]
    return df['location_name'].tolist()

def get_template_excel(template_type):
    buffer = io.BytesIO()
    if template_type == 'stock':
        df = pd.DataFrame(columns=['item', 'brand_name', 'location', 'ml_per_bottle', 'sealed_bottles', 'open_bottles',
                                   'open_ml', 'total_ml', 'bill_no', 'date', 'price', 'supplier', 'remarks', 'mfg_date', 'expiry_date'])
    elif template_type == 'event':
        df = pd.DataFrame(columns=['date', 'occasion', 'brand_name', 'location', 'bottles_consumed', 'extra_ml', 'permit_number'])
    
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Template')
    buffer.seek(0)
    return buffer.getvalue()

# ==========================================
# EXPIRY & FIFO UTILITIES
# ==========================================

def calculate_days_to_expiry(expiry_date_str):
    """Calculate days remaining until expiry. Returns negative if expired."""
    if not expiry_date_str:
        return None
    try:
        expiry = datetime.strptime(expiry_date_str, "%Y-%m-%d")
        today = datetime.today()
        days = (expiry - today).days
        return days
    except:
        return None

def get_expiry_status(days_to_expiry):
    """Return status badge and color for expiry status."""
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
    """Get stock ordered by FIFO principle (oldest/closest to expiry first)."""
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
    """Validate manufacturing and expiry dates."""
    errors = []
    
    try:
        mfg = datetime.strptime(mfg_date, "%Y-%m-%d") if mfg_date else None
        exp = datetime.strptime(expiry_date, "%Y-%m-%d") if expiry_date else None
        today = datetime.today()
        
        if mfg and mfg > today:
            errors.append("Manufacturing date cannot be in the future.")
        
        if mfg and exp and exp <= mfg:
            errors.append("Expiry date must be after manufacturing date.")
        
        if exp and exp < today:
            errors.append("⚠️ WARNING: Expiry date is already passed (Expired).")
            
    except ValueError:
        errors.append("Invalid date format. Use YYYY-MM-DD.")
    
    return errors

def rename_for_display(df):
    mapping = {
        'item_name': 'Item',
        'quantity_added': 'Bottle Count',
        'open_bottles': 'Open Bottles',
        'closed_bottles': 'Sealed Bottles',
        'total_ml_available': 'Total ML',
        'date_added': 'Date Added',
        'brand_name': 'Brand Name',
        'ml_per_bottle': 'ML per Bottle',
        'bill_no': 'Bill No',
        'price': 'Price',
        'supplier': 'Supplier',
        'remarks': 'Remarks',
        'mfg_date': 'MFG Date',
        'expiry_date': 'Expiry Date',
        'event_id': 'Event ID',
        'date': 'Date',
        'occasion': 'Occasion',
        'total_bottles_before': 'Total Bottles Before',
        'total_ml_before': 'Total ML Before',
        'ml_consumed': 'ML Consumed',
        'closed_bottles_opened': 'Sealed Bottles Opened',
        'open_ml_used': 'Open ML Used',
        'total_bottles_after': 'Total Bottles After',
        'total_ml_after': 'Total ML After',
        'open_bottles_after': 'Open Bottles After',
        'closed_bottles_after': 'Sealed Bottles After',
        'permit_number': 'Permit Number',
        'item': 'Item',
        'bottle_count': 'Bottle Count',
        'status': 'Status',
        'requested_by': 'Requested By',
        'location': 'Location',
        'fifo_note': 'FIFO Note'
    }
    return df.rename(columns=mapping)

# ==========================================
# CORE REUSABLE LOGIC
# ==========================================

def apply_consumption_logic(brand, location, ml_consumed):
    """Apply FIFO consumption logic - uses expiry-closest stock first."""
    if ml_consumed <= 0:
        return False, "Consumption amount must be greater than zero."

    conn = get_connection()
    c = conn.cursor()
    
    try:
        # Get stock ordered by FIFO (expiry date first)
        stock_rows = get_fifo_order(brand, location)
        
        if stock_rows.empty:
            raise ValueError(f"No stock available for {brand} at {location}.")
        
        # Check for expired stock and warn
        expired_rows = stock_rows[
            stock_rows['expiry_date'].apply(lambda x: calculate_days_to_expiry(x) is not None and calculate_days_to_expiry(x) < 0)
        ]
        if not expired_rows.empty:
            st.warning(f"⚠️ WARNING: {len(expired_rows)} batch(es) of {brand} have EXPIRED at {location}. These should be removed.")
            
        total_ml_before = float(stock_rows['total_ml_available'].sum())
        total_closed_before = int(stock_rows['closed_bottles'].sum())
        total_open_before = int(stock_rows['open_bottles'].sum())
        total_bottles_before = total_closed_before + total_open_before
        
        if ml_consumed > total_ml_before:
            raise ValueError(f"Insufficient stock for {brand} at {location}. Requested: {ml_consumed} ML, Available: {total_ml_before} ML.")
            
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
            expiry_str = row['expiry_date']
            
            consumed_from_row = min(remaining_to_consume, r_total_ml)
            fifo_details.append(f"Batch ({expiry_str}): -{consumed_from_row:.0f}ML")
            
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
# EVENT DELETION FUNCTION
# ==========================================

def delete_event(event_id):
    """Delete a specific event by event_id."""
    try:
        result = run_query("DELETE FROM EVENT_TABLE WHERE event_id = ?", (event_id,))
        return result
    except Exception as e:
        st.error(f"Error deleting event: {e}")
        return False

# ==========================================
# PAGE FUNCTIONS
# ==========================================

def manage_locations():
    st.title("📍 Manage Locations")
    st.info("Dynamically add or remove locations. You cannot delete a location if it currently holds stock.")
    
    # Initialize session state for deletion confirmation
    if 'pending_delete_location' not in st.session_state:
        st.session_state['pending_delete_location'] = None
    
    locations = get_active_locations()
    
    with st.form("add_loc_form", clear_on_submit=True):
        st.subheader("Add New Location")
        new_loc = st.text_input("Location Name")
        if st.form_submit_button("Add Location", type="primary"):
            if new_loc and new_loc.strip() != "":
                clean_name = new_loc.strip()
                try:
                    conn = get_connection()
                    c = conn.cursor()
                    c.execute("INSERT INTO LOCATIONS_TABLE (location_name) VALUES (?)", (clean_name,))
                    conn.commit()
                    st.success(f"✅ Added location: {clean_name}")
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.error("❌ This location name already exists.")
                finally:
                    conn.close()
            else:
                st.error("❌ Location name cannot be empty.")
                
    st.markdown("---")
    st.subheader("Active Locations")
    
    if not locations:
        st.info("No locations added yet.")
        return
    
    # Display each location with stock info and delete button
    for loc in locations:
        # Check if location has stock
        stock_check = fetch_data(
            "SELECT COUNT(*) as count FROM STOCK_TABLE WHERE location = ?", 
            (loc,)
        )
        has_stock = stock_check.iloc[0]['count'] > 0 if not stock_check.empty else False
        
        col_name, col_stock, col_delete = st.columns([2, 1, 1])
        
        with col_name:
            st.write(f"🏢 **{loc}**")
        
        with col_stock:
            if has_stock:
                st.caption(f"📦 {stock_check.iloc[0]['count']} items")
            else:
                st.caption("✅ Empty")
        
        with col_delete:
            if has_stock:
                st.button(
                    "🔒 Can't Delete",
                    key=f"locked_{loc}",
                    disabled=True,
                    use_container_width=True,
                    help="This location has active stock"
                )
            else:
                if st.button("🗑️ Delete", key=f"del_{loc}", use_container_width=True):
                    st.session_state['pending_delete_location'] = loc
    
    st.markdown("---")
    
    # Handle deletion with confirmation
    if st.session_state['pending_delete_location']:
        loc_to_delete = st.session_state['pending_delete_location']
        
        # Double check - location should be empty
        stock_check = fetch_data(
            "SELECT COUNT(*) as count FROM STOCK_TABLE WHERE location = ?", 
            (loc_to_delete,)
        )
        has_stock = stock_check.iloc[0]['count'] > 0 if not stock_check.empty else False
        
        if has_stock:
            st.error(f"❌ Cannot delete '{loc_to_delete}' - it has active stock!")
            st.session_state['pending_delete_location'] = None
        else:
            st.warning(f"⚠️ **Confirm Deletion of '{loc_to_delete}'**")
            col_yes, col_no = st.columns(2)
            
            with col_yes:
                if st.button("✅ Yes, Delete Location", type="primary", use_container_width=True, key="confirm_del_yes"):
                    try:
                        conn = get_connection()
                        c = conn.cursor()
                        c.execute("DELETE FROM LOCATIONS_TABLE WHERE location_name = ?", (loc_to_delete,))
                        conn.commit()
                        conn.close()
                        st.success(f"✅ Location '{loc_to_delete}' deleted successfully!")
                        st.session_state['pending_delete_location'] = None
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error deleting location: {e}")
                        st.session_state['pending_delete_location'] = None
            
            with col_no:
                if st.button("❌ Cancel", use_container_width=True, key="confirm_del_no"):
                    st.session_state['pending_delete_location'] = None
                    st.rerun()

def dashboard():
    st.title("📊 Dashboard & Expiry Alerts")
    
    locations = get_active_locations()
    stock_df = fetch_data("SELECT * FROM STOCK_TABLE")
    events_df = fetch_data("SELECT * FROM EVENT_TABLE")
    
    if stock_df.empty:
        st.info("No stock data available yet.")
        return
    
    # Calculate expiry metrics
    stock_df['days_to_expiry'] = stock_df['expiry_date'].apply(calculate_days_to_expiry)
    expired = stock_df[stock_df['days_to_expiry'] < 0]
    expiring_critical = stock_df[(stock_df['days_to_expiry'] >= 0) & (stock_df['days_to_expiry'] < CRITICAL_EXPIRY_DAYS)]
    expiring_warning = stock_df[(stock_df['days_to_expiry'] >= CRITICAL_EXPIRY_DAYS) & (stock_df['days_to_expiry'] < WARNING_EXPIRY_DAYS)]
    
    # A. Expiry Alert Section
    st.markdown("### 🚨 EXPIRY ALERTS")
    
    col1, col2, col3, col4 = st.columns(4)
    
    exp_bottles = len(expired)
    exp_ml = expired['total_ml_available'].sum() if not expired.empty else 0
    col1.metric("⚫ EXPIRED Bottles", f"{exp_bottles}", f"{exp_ml:,.0f} ML", delta_color="inverse")
    
    crit_bottles = len(expiring_critical)
    crit_ml = expiring_critical['total_ml_available'].sum() if not expiring_critical.empty else 0
    col2.metric("🔴 CRITICAL (< 7 days)", f"{crit_bottles}", f"{crit_ml:,.0f} ML", delta_color="inverse")
    
    warn_bottles = len(expiring_warning)
    warn_ml = expiring_warning['total_ml_available'].sum() if not expiring_warning.empty else 0
    col3.metric("🟡 WARNING (7-30 days)", f"{warn_bottles}", f"{warn_ml:,.0f} ML")
    
    safe_df = stock_df[stock_df['days_to_expiry'] >= WARNING_EXPIRY_DAYS]
    safe_bottles = len(safe_df)
    safe_ml = safe_df['total_ml_available'].sum() if not safe_df.empty else 0
    col4.metric("🟢 GOOD (> 30 days)", f"{safe_bottles}", f"{safe_ml:,.0f} ML")
    
    if not expired.empty:
        st.error("⚠️ **CRITICAL**: Following stock has EXPIRED and must be removed:")
        exp_display = rename_for_display(expired[['brand_name', 'location', 'expiry_date', 'total_ml_available']])
        st.dataframe(exp_display, use_container_width=True)
    
    st.markdown("---")
    
    # B. Overall Metrics
    st.markdown("### 🌍 Overall Inventory Summary")
    total_ml = stock_df[stock_df['days_to_expiry'] >= 0]['total_ml_available'].sum()  # Exclude expired
    total_bottles = (stock_df[stock_df['days_to_expiry'] >= 0]['open_bottles'].sum() + 
                     stock_df[stock_df['days_to_expiry'] >= 0]['closed_bottles'].sum())
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Valid Inventory (ML)", f"{total_ml:,.2f}")
    c2.metric("Valid Bottles", f"{total_bottles}")
    
    if not events_df.empty:
        top_brand_df = events_df.groupby('brand_name')['ml_consumed'].sum().reset_index()
        top_brand = top_brand_df.loc[top_brand_df['ml_consumed'].idxmax()]['brand_name']
        c3.metric("Most Consumed Brand", top_brand)
    else:
        c3.metric("Most Consumed Brand", "N/A")

    st.markdown("---")
    
    # C. Location-wise Split
    st.markdown("### 🏢 Location-wise Split")
    
    for loc in locations:
        st.markdown(f"#### 📌 {loc}")
        loc_stock = stock_df[stock_df['location'] == loc]
        loc_stock['days_to_expiry'] = loc_stock['expiry_date'].apply(calculate_days_to_expiry)
        
        if loc_stock.empty:
            st.info(f"No stock data for {loc}.")
            continue
            
        loc_ml = loc_stock[loc_stock['days_to_expiry'] >= 0]['total_ml_available'].sum()
        loc_bottles = (loc_stock[loc_stock['days_to_expiry'] >= 0]['open_bottles'].sum() + 
                       loc_stock[loc_stock['days_to_expiry'] >= 0]['closed_bottles'].sum())
        loc_sealed = loc_stock[loc_stock['days_to_expiry'] >= 0]['closed_bottles'].sum()
        loc_open = loc_stock[loc_stock['days_to_expiry'] >= 0]['open_bottles'].sum()
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total ML", f"{loc_ml:,.2f}")
        c2.metric("Total Bottles", f"{loc_bottles}")
        c3.metric("Sealed Bottles", f"{loc_sealed}")
        c4.metric("Open Bottles", f"{loc_open}")
        
        # Charts per Location
        colA, colB = st.columns(2)
        
        with colA:
            brand_stock = loc_stock[loc_stock['days_to_expiry'] >= 0].groupby('brand_name')['total_ml_available'].sum().reset_index()
            if not brand_stock.empty:
                fig_pie = px.pie(brand_stock, names='brand_name', values='total_ml_available', hole=0.3, title="Stock by Brand")
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.write("No stock data available for this location.")
            
        with colB:
            loc_events = events_df[events_df['location'] == loc] if not events_df.empty else pd.DataFrame()
            if not loc_events.empty:
                brand_cons = loc_events.groupby('brand_name')['ml_consumed'].sum().reset_index()
                fig_bar = px.bar(brand_cons, x='brand_name', y='ml_consumed', text_auto=True, title="Consumption by Brand")
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.write("No consumption data yet for this location.")
                
        st.markdown("<br>", unsafe_allow_html=True)


def add_stock():
    st.title("➕ Add Stock")
    st.info("✅ **TIP**: Always enter MFG and Expiry dates for compliance and inventory rotation.")
    
    locations = get_active_locations()
    
    col_loc, col_date = st.columns(2)
    with col_loc:
        location = st.selectbox("Location *", locations)
    with col_date:
        date_added = st.date_input("Date Added", datetime.today())
    
    col1, col2 = st.columns(2)
    
    with col1:
        item_options = ["Whiskey", "Vodka", "Beer", "Rum", "Wine", "Gin", "Tequila", "Brandy", "Champagne", "Others"]
        selected_item = st.selectbox("Item *", item_options)
        
        if selected_item == "Others":
            actual_item = st.text_input("Specify Item *")
        else:
            actual_item = selected_item
            
        brand_name = st.text_input("Brand Name *")
        price = st.number_input("Price", min_value=0.0, step=10.0)
        
    with col2:
        ml_per_bottle = st.number_input("ML per Bottle *", min_value=1.0, value=750.0, step=50.0)
        supplier = st.text_input("Supplier")
        remarks = st.text_area("Remarks")
    
    st.markdown("---")
    st.markdown("### 🍾 Bottle Type Selection")
    bottle_type = st.radio("Select Bottle Type", ["🔒 Sealed Only", "🍷 Open Only", "📦 Both Sealed & Open"], horizontal=True)
    
    st.markdown("---")
    
    # Initialize values
    closed_bottles = 0
    open_bottles = 0
    open_ml = 0.0
    total_ml_available = 0.0
    
    if bottle_type == "🔒 Sealed Only":
        st.markdown("### 📦 Sealed Bottles")
        col_sealed = st.columns(1)[0]
        with col_sealed:
            closed_bottles = st.number_input("Number of Sealed Bottles *", min_value=1, value=1, step=1, key="sealed_only")
        open_bottles = 0
        open_ml = 0.0
        total_ml_available = closed_bottles * ml_per_bottle
        
    elif bottle_type == "🍷 Open Only":
        st.markdown("### 🍷 Open Bottle Details")
        col_open_ml, col_open_count = st.columns(2)
        with col_open_ml:
            open_ml = st.number_input("Open ML Available *", min_value=1.0, value=100.0, step=10.0, key="open_ml_only")
        with col_open_count:
            open_bottles = st.number_input("Number of Open Bottles", min_value=0, value=0, step=1, key="open_count")
        
        closed_bottles = 0
        total_ml_available = open_ml
        
    else:  # Both
        st.markdown("#### 📦 Sealed Bottles")
        col_sealed = st.columns(1)[0]
        with col_sealed:
            closed_bottles = st.number_input("Number of Sealed Bottles", min_value=0, value=0, step=1, key="sealed_both")
        
        st.markdown("#### 🍷 Open Bottle Details")
        col_open_ml, col_open_count = st.columns(2)
        with col_open_ml:
            open_ml = st.number_input("Open ML Available", min_value=0.0, value=0.0, step=10.0, key="open_ml_both")
        with col_open_count:
            open_bottles = st.number_input("Number of Open Bottles", min_value=0, value=0, step=1, key="open_count_both")
        
        total_ml_available = (closed_bottles * ml_per_bottle) + open_ml
    
    # Display Summary
    st.markdown("---")
    st.markdown("### 📊 Summary")
    col_summary1, col_summary2, col_summary3 = st.columns(3)
    with col_summary1:
        st.metric("🔒 Sealed Bottles", closed_bottles)
    with col_summary2:
        st.metric("🍷 Open Bottles", open_bottles)
    with col_summary3:
        st.metric("📏 Total ML", f"{total_ml_available:.0f}")
        
    # DATE FIELDS
    st.markdown("---")
    st.markdown("### 📅 Manufacturing & Expiry Dates")
    col_mfg, col_exp = st.columns(2)
    
    with col_mfg:
        mfg_date = st.date_input("Manufacturing Date *", value=None, format="YYYY-MM-DD")
    with col_exp:
        exp_date = st.date_input("Expiry Date *", value=None, format="YYYY-MM-DD")
    
    # Validate dates
    if mfg_date and exp_date:
        errors = validate_dates(mfg_date.strftime("%Y-%m-%d"), exp_date.strftime("%Y-%m-%d"))
        if errors:
            for err in errors:
                st.warning(err)
        else:
            days_to_exp = calculate_days_to_expiry(exp_date.strftime("%Y-%m-%d"))
            status, color = get_expiry_status(days_to_exp)
            st.info(f"✅ Status: {status} | ⏱️ Days to Expiry: {days_to_exp}")
        
    if st.button("💾 Save Stock", type="primary", use_container_width=True):
        if not brand_name or not actual_item:
            st.error("❌ Item and Brand Name are required.")
        elif not mfg_date or not exp_date:
            st.error("❌ Manufacturing Date and Expiry Date are required.")
        elif closed_bottles == 0 and open_bottles == 0:
            st.error("❌ At least one bottle (sealed or open) is required.")
        else:
            errors = validate_dates(mfg_date.strftime("%Y-%m-%d"), exp_date.strftime("%Y-%m-%d"))
            if errors:
                st.error("\n".join(errors))
            else:
                quantity_added = closed_bottles + open_bottles
                
                run_query("INSERT OR IGNORE INTO BRAND_MASTER (brand_name, standard_ml, category) VALUES (?, ?, ?)", 
                          (brand_name.strip(), ml_per_bottle, actual_item.strip()))
                
                query = '''
                    INSERT INTO STOCK_TABLE 
                    (date_added, brand_name, item_name, ml_per_bottle, quantity_added, open_bottles, 
                     closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks, location, mfg_date, expiry_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                success = run_query(query, (
                    date_added.strftime("%Y-%m-%d"), brand_name.strip(), actual_item.strip(), ml_per_bottle, 
                    quantity_added, open_bottles, closed_bottles, open_ml, total_ml_available, 
                    "", price, supplier.strip(), remarks.strip(), location, 
                    mfg_date.strftime("%Y-%m-%d"), exp_date.strftime("%Y-%m-%d")
                ))
                if success:
                    st.success(f"✅ Successfully added {quantity_added} bottles of {brand_name} ({actual_item}) to {location}.\n📅 Expiry: {exp_date.strftime('%Y-%m-%d')}")

def view_stock():
    st.title("📦 View Stock")
    
    df = fetch_data("SELECT * FROM STOCK_TABLE ORDER BY expiry_date ASC, date_added DESC")
    locations = ["All Locations"] + get_active_locations()
    
    if df.empty:
        st.info("No stock data found.")
        return
    
    # Calculate expiry info
    df['days_to_expiry'] = df['expiry_date'].apply(calculate_days_to_expiry)
    df['expiry_status'] = df['days_to_expiry'].apply(lambda x: get_expiry_status(x)[0])
        
    col_loc, col_item, col_status = st.columns(3)
    with col_loc:
        filter_loc = st.selectbox("Filter by Location", locations)
    with col_item:
        item_types = ["All"] + sorted(df['item_name'].unique().tolist())
        filter_item = st.selectbox("Filter by Item Type", item_types)
    with col_status:
        statuses = ["All", "🟢 GOOD", "🟡 EXPIRING SOON", "🔴 CRITICAL", "⚫ EXPIRED"]
        filter_status = st.selectbox("Filter by Expiry Status", statuses)
        
    if filter_loc != "All Locations":
        df = df[df['location'] == filter_loc]
    if filter_item != "All":
        df = df[df['item_name'] == filter_item]
    if filter_status != "All":
        df = df[df['expiry_status'] == filter_status]
        
    df_display = rename_for_display(df)
    st.dataframe(df_display, use_container_width=True, height=400)
    
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df_display.to_excel(writer, index=False, sheet_name='Stock')
    st.download_button(label="📥 Download as Excel", data=buffer.getvalue(), file_name="stock_data.xlsx", mime="application/vnd.ms-excel")

def upload_stock_excel():
    st.title("📁 Upload Stock Excel")
    
    st.download_button(
        label="📥 Download Stock Template", 
        data=get_template_excel('stock'), 
        file_name="stock_template.xlsx", 
        mime="application/vnd.ms-excel"
    )
    
    st.write("Upload an Excel file to bulk add stock.")
    st.info("📋 **Required Columns**: item, brand_name, location, ml_per_bottle, sealed_bottles, open_bottles, open_ml, total_ml, bill_no, date, price, supplier, remarks, mfg_date, expiry_date")
    st.info("📌 **Date Format**: YYYY-MM-DD (e.g., 2026-04-08)")
    
    uploaded_file = st.file_uploader("Choose a .xlsx file (Stock)", type=["xlsx"])
    active_locations = get_active_locations()
    default_loc = active_locations[0] if active_locations else "Cenpeid Guest House"
    
    if uploaded_file is not None:
        try:
            df = pd.read_excel(uploaded_file)
            st.write("📄 File Preview:")
            st.dataframe(df.head(10))
            
            # Validate required columns
            required_columns = [
                'item', 'brand_name', 'location', 'ml_per_bottle', 'sealed_bottles', 'open_bottles',
                'open_ml', 'total_ml', 'bill_no', 'date', 'price', 'supplier', 'remarks', 'mfg_date', 'expiry_date'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                st.error(f"❌ Missing required columns: {', '.join(missing_columns)}")
                st.info(f"📋 Expected columns: {', '.join(required_columns)}")
                return
            
            if st.button("Import Stock Data", type="primary"):
                success_count = 0
                failed_count = 0
                errors_log = []
                
                for index, row in df.iterrows():
                    try:
                        # Extract values from columns
                        date_added = str(row['date'])[:10] if pd.notna(row['date']) else datetime.today().strftime("%Y-%m-%d")
                        brand_name = str(row['brand_name']).strip() if pd.notna(row['brand_name']) else None
                        item_name = str(row['item']).strip() if pd.notna(row['item']) else None
                        ml_per_bottle = float(row['ml_per_bottle']) if pd.notna(row['ml_per_bottle']) else 750.0
                        sealed_bottles = int(row['sealed_bottles']) if pd.notna(row['sealed_bottles']) else 0
                        open_bottles = int(row['open_bottles']) if pd.notna(row['open_bottles']) else 0
                        open_ml = float(row['open_ml']) if pd.notna(row['open_ml']) else 0.0
                        total_ml = float(row['total_ml']) if pd.notna(row['total_ml']) else 0.0
                        bill_no = str(row['bill_no']).strip() if pd.notna(row['bill_no']) else ""
                        price = float(row['price']) if pd.notna(row['price']) else 0.0
                        supplier = str(row['supplier']).strip() if pd.notna(row['supplier']) else ""
                        remarks = str(row['remarks']).strip() if pd.notna(row['remarks']) else ""
                        
                        location = str(row['location']).strip() if pd.notna(row['location']) else default_loc
                        if location not in active_locations:
                            location = default_loc
                        
                        mfg_date = str(row['mfg_date'])[:10] if pd.notna(row['mfg_date']) else None
                        expiry_date = str(row['expiry_date'])[:10] if pd.notna(row['expiry_date']) else None
                        
                        # Validation
                        if not brand_name or not item_name:
                            errors_log.append(f"Row {index + 2}: Brand Name or Item missing")
                            failed_count += 1
                            continue
                        
                        if not mfg_date or not expiry_date:
                            errors_log.append(f"Row {index + 2}: MFG Date or Expiry Date missing")
                            failed_count += 1
                            continue
                        
                        # Validate dates
                        date_errors = validate_dates(mfg_date, expiry_date)
                        if date_errors:
                            errors_log.append(f"Row {index + 2}: {date_errors[0]}")
                            failed_count += 1
                            continue
                        
                        # Add to BRAND_MASTER
                        run_query(
                            "INSERT OR IGNORE INTO BRAND_MASTER (brand_name, standard_ml, category) VALUES (?, ?, ?)",
                            (brand_name, ml_per_bottle, item_name)
                        )
                        
                        # Insert into STOCK_TABLE
                        quantity_added = sealed_bottles + open_bottles
                        query = '''
                            INSERT INTO STOCK_TABLE 
                            (date_added, brand_name, item_name, ml_per_bottle, quantity_added, open_bottles, 
                             closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks, 
                             location, mfg_date, expiry_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        '''
                        
                        if run_query(query, (
                            date_added, brand_name, item_name, ml_per_bottle, quantity_added,
                            open_bottles, sealed_bottles, open_ml, total_ml, bill_no, price,
                            supplier, remarks, location, mfg_date, expiry_date
                        )):
                            success_count += 1
                        else:
                            failed_count += 1
                    
                    except Exception as e:
                        errors_log.append(f"Row {index + 2}: {str(e)}")
                        failed_count += 1
                
                # Display results
                st.success(f"✅ Successfully imported {success_count} rows!")
                
                if failed_count > 0:
                    st.warning(f"⚠️ Failed to import {failed_count} rows")
                    with st.expander(f"📋 View {len(errors_log)} Errors"):
                        for error in errors_log[:20]:  # Show first 20 errors
                            st.write(f"• {error}")
                        if len(errors_log) > 20:
                            st.write(f"... and {len(errors_log) - 20} more errors")
        
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")
            st.info("Please ensure your Excel file has the correct column names and format.")

def create_event():
    st.title("🎉 Create Event (Consumption)")
    st.info("💡 **FIFO Applied**: Oldest/expiring-soonest stock will be consumed first for automatic inventory rotation.")
    
    locations = get_active_locations()
    location = st.selectbox("Select Location *", locations)
    
    brands_df = fetch_data("SELECT DISTINCT brand_name FROM STOCK_TABLE WHERE total_ml_available > 0 AND location = ?", (location,))
    if brands_df.empty:
        st.warning(f"No available stock to consume at {location}.")
        return
        
    brand = st.selectbox("Select Brand to Consume", brands_df['brand_name'].tolist())
    
    stock_rows = fetch_data("SELECT SUM(closed_bottles) as c, SUM(open_bottles) as o, SUM(total_ml_available) as t, MAX(ml_per_bottle) as m FROM STOCK_TABLE WHERE brand_name = ? AND location = ? AND total_ml_available > 0", (brand, location))
    
    if not stock_rows.empty and stock_rows.iloc[0]['t'] is not None:
        total_ml_before = stock_rows.iloc[0]['t']
        total_closed_before = stock_rows.iloc[0]['c']
        total_open_before = stock_rows.iloc[0]['o']
        ml_per_bottle = stock_rows.iloc[0]['m']
        total_bottles_before = total_closed_before + total_open_before
        
        st.info(f"📦 **Current Stock available at {location}:** {total_bottles_before} Bottles ({total_closed_before} Sealed, {total_open_before} Open) | {total_ml_before:,.2f} Total ML")
        
        # Show FIFO order
        fifo_df = get_fifo_order(brand, location)
        fifo_df['days_to_expiry'] = fifo_df['expiry_date'].apply(calculate_days_to_expiry)
        fifo_df['expiry_status'] = fifo_df['days_to_expiry'].apply(lambda x: get_expiry_status(x)[0])
        
        st.markdown("#### 📋 Consumption Order (FIFO):")
        fifo_display = rename_for_display(fifo_df[['mfg_date', 'expiry_date', 'days_to_expiry', 'expiry_status', 'closed_bottles', 'open_bottles', 'total_ml_available']])
        st.dataframe(fifo_display, use_container_width=True)
        
        date = st.date_input("Event Date", datetime.today())
        occasion = st.text_input("Occasion / Event Name")
        permit_number = st.text_input("Permit Number (e.g., P10)")
        
        st.markdown("### Consumption Details")
        auto_calc = st.toggle("Auto calculate from bottles only", value=False)
        
        col_b, col_m = st.columns(2)
        with col_b:
            bottles_consumed = st.number_input("Bottles Consumed", min_value=0, step=1)
        with col_m:
            extra_ml = st.number_input("Extra ML Consumed", min_value=0.0, step=10.0, disabled=auto_calc)
            if auto_calc:
                extra_ml = 0.0

        total_ml_consumed = (bottles_consumed * ml_per_bottle) + extra_ml
        
        st.markdown("---")
        st.write(f"ℹ️ *1 bottle = {ml_per_bottle} ML*")
        st.write(f"🩸 **Total ML to be consumed = {total_ml_consumed:,.2f} ML**")
        
        is_valid = True
        if total_ml_consumed > total_ml_before:
            st.error(f"⚠️ Consumption ({total_ml_consumed:,.2f} ML) exceeds available stock ({total_ml_before:,.2f} ML)!")
            is_valid = False
        elif extra_ml >= ml_per_bottle:
            st.error(f"⚠️ Extra ML ({extra_ml}) should be less than bottle size ({ml_per_bottle}). Please add another bottle instead.")
            is_valid = False
        elif total_ml_consumed == 0:
            st.warning("⚠️ Please enter a consumption amount greater than zero.")
            is_valid = False
            
        if is_valid:
            preview_ml = total_ml_before - total_ml_consumed
            est_sealed = math.floor(preview_ml / ml_per_bottle)
            est_open = 1 if (preview_ml % ml_per_bottle) > 0 else 0
            est_total = est_sealed + est_open
            st.success(f"📊 **Remaining AFTER Event (Preview):** ~{est_total} Bottles left | {preview_ml:,.2f} ML left")
            
            if st.button("Record Consumption", type="primary"):
                success, result = apply_consumption_logic(brand, location, total_ml_consumed)
                
                if success:
                    stats = result
                    insert_q = '''
                        INSERT INTO EVENT_TABLE 
                        (date, occasion, brand_name, total_bottles_before, total_ml_before, ml_consumed, 
                         closed_bottles_opened, open_ml_used, total_bottles_after, total_ml_after, 
                         open_bottles_after, closed_bottles_after, permit_number, location, fifo_note)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    run_query(insert_q, (
                        date.strftime("%Y-%m-%d"), occasion, brand, stats['total_bottles_before'], stats['total_ml_before'], 
                        total_ml_consumed, stats['closed_bottles_opened'], stats['open_ml_used'], stats['total_bottles_after'], 
                        stats['total_ml_after'], stats['open_bottles_after'], stats['closed_bottles_after'], permit_number, location,
                        stats['fifo_note']
                    ))
                    st.success("✅ Consumption recorded successfully!")
                    st.info(f"📝 FIFO Details:\n{stats['fifo_note']}")
                else:
                    st.error(f"Error during consumption: {result}")

def expiry_report():
    st.title("📊 Expiry & Compliance Report")
    
    locations = get_active_locations()
    filter_loc = st.selectbox("Filter by Location", ["All Locations"] + locations)
    
    stock_df = fetch_data("SELECT * FROM STOCK_TABLE")
    
    if stock_df.empty:
        st.info("No stock data.")
        return
    
    stock_df['days_to_expiry'] = stock_df['expiry_date'].apply(calculate_days_to_expiry)
    stock_df['expiry_status'] = stock_df['days_to_expiry'].apply(lambda x: get_expiry_status(x)[0])
    
    if filter_loc != "All Locations":
        stock_df = stock_df[stock_df['location'] == filter_loc]
    
    # Summary Metrics
    st.markdown("### 📈 Summary Metrics")
    
    expired_df = stock_df[stock_df['days_to_expiry'] < 0]
    critical_df = stock_df[(stock_df['days_to_expiry'] >= 0) & (stock_df['days_to_expiry'] < CRITICAL_EXPIRY_DAYS)]
    warning_df = stock_df[(stock_df['days_to_expiry'] >= CRITICAL_EXPIRY_DAYS) & (stock_df['days_to_expiry'] < WARNING_EXPIRY_DAYS)]
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Expired", len(expired_df))
    c2.metric("Critical (< 7 days)", len(critical_df))
    c3.metric("Warning (7-30 days)", len(warning_df))
    c4.metric("Total Stock Items", len(stock_df))
    
    st.markdown("---")
    st.markdown("### 🚨 Expired Stock")
    if not expired_df.empty:
        exp_display = rename_for_display(expired_df[['brand_name', 'location', 'mfg_date', 'expiry_date', 'closed_bottles', 'open_bottles', 'total_ml_available']])
        st.dataframe(exp_display, use_container_width=True)
    else:
        st.success("✅ No expired stock")
    
    st.markdown("### 🔴 Critical (Expires in < 7 days)")
    if not critical_df.empty:
        crit_display = rename_for_display(critical_df[['brand_name', 'location', 'mfg_date', 'expiry_date', 'days_to_expiry', 'closed_bottles', 'open_bottles', 'total_ml_available']])
        st.dataframe(crit_display, use_container_width=True)
    else:
        st.success("✅ No critical expirations")
    
    st.markdown("### 🟡 Warning (Expires in 7-30 days)")
    if not warning_df.empty:
        warn_display = rename_for_display(warning_df[['brand_name', 'location', 'mfg_date', 'expiry_date', 'days_to_expiry', 'closed_bottles', 'open_bottles', 'total_ml_available']])
        st.dataframe(warn_display, use_container_width=True)
    else:
        st.success("✅ No warnings")
    
    # Export
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        if not expired_df.empty:
            expired_df[['brand_name', 'location', 'mfg_date', 'expiry_date', 'total_ml_available']].to_excel(writer, index=False, sheet_name='Expired')
        if not critical_df.empty:
            critical_df[['brand_name', 'location', 'mfg_date', 'expiry_date', 'total_ml_available']].to_excel(writer, index=False, sheet_name='Critical')
        if not warning_df.empty:
            warning_df[['brand_name', 'location', 'mfg_date', 'expiry_date', 'total_ml_available']].to_excel(writer, index=False, sheet_name='Warning')
    
    st.download_button(label="📥 Download Compliance Report", data=buffer.getvalue(), file_name="expiry_report.xlsx", mime="application/vnd.ms-excel")

def event_history():
    st.title("📜 Event History (with FIFO Notes)")
    
    df = fetch_data("SELECT * FROM EVENT_TABLE ORDER BY event_id DESC")
    locations = ["All Locations"] + get_active_locations()
    
    if df.empty:
        st.info("No events recorded yet.")
        return
        
    col_loc, col_brand, col_occ = st.columns(3)
    with col_loc:
        filter_loc = st.selectbox("Filter by Location", locations)
    with col_brand:
        brands = ["All"] + df['brand_name'].unique().tolist()
        filter_brand = st.selectbox("Filter by Brand", brands, key="ev_brand")
    with col_occ:
        occasions = ["All"] + df['occasion'].unique().tolist()
        filter_occ = st.selectbox("Filter by Occasion", occasions)
        
    if filter_loc != "All Locations":
        df = df[df['location'] == filter_loc]
    if filter_brand != "All":
        df = df[df['brand_name'] == filter_brand]
    if filter_occ != "All":
        df = df[df['occasion'] == filter_occ]
    
    # Display with delete buttons for admin users
    if st.session_state.get('user_role') == 'admin':
        st.warning("⚠️ **Admin Mode**: Delete functionality enabled below")
        
        for index, row in df.iterrows():
            col_data, col_delete = st.columns([5, 1])
            
            with col_data:
                st.write(f"**Event #{row['event_id']}** | {row['date']} | {row['occasion']} | {row['brand_name']} | {row['location']} | {row['ml_consumed']:.0f}ML")
            
            with col_delete:
                if st.button("🗑️ Delete", key=f"del_event_{row['event_id']}", use_container_width=True):
                    with st.popover("Confirm Delete"):
                        st.warning(f"Delete Event #{row['event_id']} from {row['date']}?")
                        if st.button("Yes, Delete", type="primary", key=f"confirm_del_{row['event_id']}"):
                            if delete_event(row['event_id']):
                                st.success(f"✅ Event #{row['event_id']} deleted successfully!")
                                st.rerun()
                            else:
                                st.error("Failed to delete event.")
    else:
        # Normal user view (read-only)
        df_display = rename_for_display(df)
        st.dataframe(df_display, use_container_width=True)

    # Export button (for both users)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        rename_for_display(df).to_excel(writer, index=False, sheet_name='Events')
    st.download_button(label="📥 Download Events as Excel", data=buffer.getvalue(), file_name="event_history.xlsx", mime="application/vnd.ms-excel")

def brand_summary():
    st.title("📋 Brand Summary")
    
    locations = ["All Locations"] + get_active_locations()
    filter_loc = st.selectbox("Filter by Location", locations)
    
    where_clause = ""
    params = ()
    if filter_loc != "All Locations":
        where_clause = "WHERE location = ?"
        params = (filter_loc,)
        
    query = f'''
        SELECT 
            brand_name, 
            location,
            MAX(ml_per_bottle) as ml_per_bottle,
            MIN(expiry_date) as earliest_expiry,
            SUM(quantity_added) as total_bottles_added,
            SUM(closed_bottles) as current_closed_bottles,
            SUM(open_bottles) as current_open_bottles,
            SUM(open_ml) as current_open_ml,
            SUM(total_ml_available) as current_total_ml
        FROM STOCK_TABLE
        {where_clause}
        GROUP BY brand_name, location
    '''
    df = fetch_data(query, params)
    
    if df.empty:
        st.info("No data available.")
        return
    
    df['days_to_expiry'] = df['earliest_expiry'].apply(calculate_days_to_expiry)
    df['total_current_bottles'] = df['current_closed_bottles'] + df['current_open_bottles']
    df['expiry_status'] = df['days_to_expiry'].apply(lambda x: get_expiry_status(x)[0])
    
    summary_mapping = {
        'brand_name': 'Brand Name',
        'location': 'Location',
        'ml_per_bottle': 'ML per Bottle',
        'earliest_expiry': 'Earliest Expiry',
        'expiry_status': 'Status',
        'total_bottles_added': 'Total Bottles Added',
        'current_closed_bottles': 'Sealed Bottles',
        'current_open_bottles': 'Open Bottles',
        'total_current_bottles': 'Total Bottles Left',
        'current_open_ml': 'Open ML Left',
        'current_total_ml': 'Total ML Left'
    }
    df_display = df.rename(columns=summary_mapping)
    st.dataframe(df_display, use_container_width=True)

def request_stock_addition():
    st.title("📝 Request Stock Addition")
    st.info("Submit a request for new stock. Include MFG and Expiry dates for compliance.")
    
    locations = get_active_locations()
    
    col_loc, col_date = st.columns(2)
    with col_loc:
        location = st.selectbox("Location *", locations)
    with col_date:
        date_added = st.date_input("Date", datetime.today())
    
    col1, col2 = st.columns(2)
    
    with col1:
        item_options = ["Whiskey", "Vodka", "Beer", "Rum", "Wine", "Gin", "Tequila", "Brandy", "Champagne", "Others"]
        selected_item = st.selectbox("Item *", item_options)
        
        if selected_item == "Others":
            actual_item = st.text_input("Specify Item *")
        else:
            actual_item = selected_item
            
        brand_name = st.text_input("Brand Name *")
        price = st.number_input("Price", min_value=0.0, step=10.0)
        bottle_count = st.number_input("Bottle Count *", min_value=1, value=1, step=1)
        
    with col2:
        ml_per_bottle = st.number_input("ML per Bottle *", min_value=1.0, value=750.0, step=50.0)
        supplier = st.text_input("Supplier")
        remarks = st.text_area("Remarks")
    
    st.markdown("### 📅 Manufacturing & Expiry Dates")
    col_mfg, col_exp = st.columns(2)
    
    with col_mfg:
        mfg_date = st.date_input("Manufacturing Date *", value=None, format="YYYY-MM-DD")
    with col_exp:
        exp_date = st.date_input("Expiry Date *", value=None, format="YYYY-MM-DD")
        
    if st.button("Send Request", type="primary"):
        if not brand_name or not actual_item:
            st.error("Item and Brand Name are required.")
        elif not mfg_date or not exp_date:
            st.error("Manufacturing Date and Expiry Date are required.")
        else:
            errors = validate_dates(mfg_date.strftime("%Y-%m-%d"), exp_date.strftime("%Y-%m-%d"))
            if errors:
                st.error("\n".join(errors))
            else:
                query = '''
                    INSERT INTO PENDING_STOCK_TABLE 
                    (item, brand_name, bottle_count, ml_per_bottle, price, supplier, remarks, status, requested_by, date, location, mfg_date, expiry_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                success = run_query(query, (
                    actual_item.strip(), brand_name.strip(), bottle_count, ml_per_bottle, price, 
                    supplier.strip(), remarks.strip(), 'pending', 'Normal User', date_added.strftime("%Y-%m-%d"), location,
                    mfg_date.strftime("%Y-%m-%d"), exp_date.strftime("%Y-%m-%d")
                ))
                if success:
                    st.success("✅ Request sent for admin approval!")

def approve_requests():
    st.title("✅ Approve Stock Requests")
    
    pending_df = fetch_data("SELECT * FROM PENDING_STOCK_TABLE WHERE status='pending'")
    
    if pending_df.empty:
        st.info("No pending stock requests at this time.")
        return
        
    for index, row in pending_df.iterrows():
        with st.expander(f"Request #{row['request_id']} | {row['location']} | {row['bottle_count']}x {row['brand_name']} ({row['item']})", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Location:** {row['location']}")
                st.write(f"**Item:** {row['item']}")
                st.write(f"**Brand:** {row['brand_name']}")
                st.write(f"**Bottle Count:** {row['bottle_count']}")
                st.write(f"**ML per Bottle:** {row['ml_per_bottle']} ML")
            with col2:
                st.write(f"**Price:** ₹{row['price']}")
                st.write(f"**Supplier:** {row['supplier'] if row['supplier'] else 'N/A'}")
                st.write(f"**Date:** {row['date']}")
                st.write(f"**MFG Date:** {row['mfg_date'] if row['mfg_date'] else 'N/A'}")
                st.write(f"**Expiry Date:** {row['expiry_date'] if row['expiry_date'] else 'N/A'}")
                st.write(f"**Remarks:** {row['remarks'] if row['remarks'] else 'N/A'}")
                
            colA, colB = st.columns([1, 1])
            with colA:
                if st.button("Approve", key=f"approve_{row['request_id']}", type="primary", use_container_width=True):
                    open_bottles = 0
                    closed_bottles = row['bottle_count']
                    open_ml = 0.0
                    total_ml_available = row['bottle_count'] * row['ml_per_bottle']
                    
                    run_query("INSERT OR IGNORE INTO BRAND_MASTER (brand_name, standard_ml, category) VALUES (?, ?, ?)", 
                              (row['brand_name'].strip(), row['ml_per_bottle'], row['item'].strip()))
                    
                    stock_query = '''
                        INSERT INTO STOCK_TABLE 
                        (date_added, brand_name, item_name, ml_per_bottle, quantity_added, open_bottles, 
                         closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks, location, mfg_date, expiry_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    success = run_query(stock_query, (
                        row['date'], row['brand_name'].strip(), row['item'].strip(), row['ml_per_bottle'], 
                        row['bottle_count'], open_bottles, closed_bottles, open_ml, total_ml_available, 
                        "", row['price'], row['supplier'], row['remarks'], row['location'],
                        row['mfg_date'], row['expiry_date']
                    ))
                    
                    if success:
                        run_query("DELETE FROM PENDING_STOCK_TABLE WHERE request_id = ?", (row['request_id'],))
                        st.success(f"Request #{row['request_id']} approved and added to stock.")
                        st.rerun()

            with colB:
                if st.button("Reject", key=f"reject_{row['request_id']}", use_container_width=True):
                    run_query("DELETE FROM PENDING_STOCK_TABLE WHERE request_id = ?", (row['request_id'],))
                    st.warning(f"Request #{row['request_id']} rejected.")
                    st.rerun()

def edit_delete_data():
    st.title("⚙️ Edit / Delete Data")
    st.warning("⚠️ Edits here modify the raw database tables. Be careful. Ensure location fields perfectly match active locations.")
    
    tab1, tab2 = st.tabs(["Stock Data", "Event Data"])
    
    with tab1:
        st.subheader("Edit Stock")
        stock_df = fetch_data("SELECT * FROM STOCK_TABLE")
        if not stock_df.empty:
            edited_stock = st.data_editor(stock_df, num_rows="dynamic", key="stock_editor")
            if st.button("Save Stock Changes"):
                conn = get_connection()
                c = conn.cursor()
                try:
                    c.execute("DELETE FROM STOCK_TABLE")
                    edited_stock.to_sql("STOCK_TABLE", conn, if_exists="append", index=False)
                    conn.commit()
                    st.success("Stock data updated successfully!")
                except Exception as e:
                    conn.rollback()
                    st.error(f"Error saving data: {e}")
                finally:
                    conn.close()
        else:
            st.info("No stock data.")

    with tab2:
        st.subheader("Edit Events")
        event_df = fetch_data("SELECT * FROM EVENT_TABLE")
        if not event_df.empty:
            edited_events = st.data_editor(event_df, num_rows="dynamic", key="event_editor")
            if st.button("Save Event Changes"):
                conn = get_connection()
                c = conn.cursor()
                try:
                    c.execute("DELETE FROM EVENT_TABLE")
                    edited_events.to_sql("EVENT_TABLE", conn, if_exists="append", index=False)
                    conn.commit()
                    st.success("Event data updated successfully!")
                except Exception as e:
                    conn.rollback()
                    st.error(f"Error saving data: {e}")
                finally:
                    conn.close()
        else:
            st.info("No event data.")

# ==========================================
# MAIN APP NAVIGATION & LOGIN FLOW
# ==========================================

def main():
    st.set_page_config(page_title="Liquor Inventory Tracker", page_icon="🍷", layout="wide")
    init_db()
    
    if 'user_role' not in st.session_state:
        st.session_state['user_role'] = None

    if st.session_state['user_role'] is None:
        st.title("🍷 Liquor Inventory System")
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("👤 User Access")
            st.write("View dashboard, stock, events, and request new stock additions.")
            if st.button("Continue as Normal User", type="secondary"):
                st.session_state['user_role'] = 'user'
                st.rerun()
                
        with col2:
            st.subheader("🛡️ Admin Login")
            st.write("Full access to manage inventory, events, locations, and approve requests.")
            admin_user = st.text_input("Username")
            admin_pass = st.text_input("Password", type="password")
            if st.button("Login as Admin", type="primary"):
                if admin_user == ADMIN_USER and admin_pass == ADMIN_PASS:
                    st.session_state['user_role'] = 'admin'
                    st.rerun()
                else:
                    st.error("Invalid Admin Credentials")
        return

    st.sidebar.title("🍷 Liquor Tracker")
    st.sidebar.write(f"Logged in as: **{st.session_state['user_role'].title()}**")
    
    if st.sidebar.button("Logout"):
        st.session_state['user_role'] = None
        st.rerun()
        
    st.sidebar.markdown("---")
    
    if st.session_state['user_role'] == 'admin':
        pending_count_df = fetch_data("SELECT COUNT(*) as count FROM PENDING_STOCK_TABLE WHERE status='pending'")
        if not pending_count_df.empty and pending_count_df.iloc[0]['count'] > 0:
            count = pending_count_df.iloc[0]['count']
            st.sidebar.warning(f"🔔 You have {count} pending stock requests")
            
        menu = [
            "Dashboard", 
            "Manage Locations",
            "Add Stock", 
            "View Stock", 
            "Upload Stock Excel", 
            "Create Event", 
            "Event History",
            "Expiry & Compliance Report",
            "Brand Summary", 
            "Approve Requests",
            "Edit / Delete Data"
        ]
    else:
        menu = [
            "Dashboard", 
            "View Stock", 
            "Request Stock Addition",
            "Event History"
        ]
        
    choice = st.sidebar.radio("Navigation", menu)
    
    if choice == "Dashboard":
        dashboard()
    elif choice == "Manage Locations":
        manage_locations()
    elif choice == "Add Stock":
        add_stock()
    elif choice == "View Stock":
        view_stock()
    elif choice == "Upload Stock Excel":
        upload_stock_excel()
    elif choice == "Create Event":
        create_event()
    elif choice == "Event History":
        event_history()
    elif choice == "Expiry & Compliance Report":
        expiry_report()
    elif choice == "Brand Summary":
        brand_summary()
    elif choice == "Edit / Delete Data":
        edit_delete_data()
    elif choice == "Request Stock Addition":
        request_stock_addition()
    elif choice == "Approve Requests":
        approve_requests()

if __name__ == '__main__':
    main()
