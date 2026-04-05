# app.py
import streamlit as st
import sqlite3
import pandas as pd
import math
from datetime import datetime
import plotly.express as px
import io

# ==========================================
# CONFIG & CREDENTIALS
# ==========================================
ADMIN_USER = "barstock_tatapower"
ADMIN_PASS = "amittpddl"

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
    
    # Insert Default Locations if empty
    c.execute("INSERT OR IGNORE INTO LOCATIONS_TABLE (location_name) VALUES ('Cenpeid Guest House')")
    c.execute("INSERT OR IGNORE INTO LOCATIONS_TABLE (location_name) VALUES ('Civil Lines Guest House')")

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
            location TEXT
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
            location TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS BRAND_MASTER (
            brand_name TEXT PRIMARY KEY,
            standard_ml REAL,
            category TEXT
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
            location TEXT
        )
    ''')
    
    # Safe migrations for existing databases to add columns if they don't exist
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
        df = pd.DataFrame(columns=['item', 'brand_name', 'location', 'ml_per_bottle', 'bottle_count', 'bill_no', 'date', 'price', 'supplier', 'remarks'])
    elif template_type == 'event':
        df = pd.DataFrame(columns=['date', 'occasion', 'brand_name', 'location', 'bottles_consumed', 'extra_ml', 'permit_number'])
    
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Template')
    return buffer.getvalue()

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
        'location': 'Location'
    }
    return df.rename(columns=mapping)

# ==========================================
# CORE REUSABLE LOGIC
# ==========================================

def apply_consumption_logic(brand, location, ml_consumed):
    if ml_consumed <= 0:
        return False, "Consumption amount must be greater than zero."

    conn = get_connection()
    c = conn.cursor()
    
    try:
        stock_rows = pd.read_sql_query(
            "SELECT * FROM STOCK_TABLE WHERE brand_name = ? AND location = ? AND total_ml_available > 0 ORDER BY date_added ASC", 
            conn, params=(brand, location)
        )
        
        if stock_rows.empty:
            raise ValueError(f"No stock available for {brand} at {location}.")
            
        total_ml_before = float(stock_rows['total_ml_available'].sum())
        total_closed_before = int(stock_rows['closed_bottles'].sum())
        total_open_before = int(stock_rows['open_bottles'].sum())
        total_bottles_before = total_closed_before + total_open_before
        
        if ml_consumed > total_ml_before:
            raise ValueError(f"Insufficient stock for {brand} at {location}. Requested: {ml_consumed} ML, Available: {total_ml_before} ML.")
            
        remaining_to_consume = float(ml_consumed)
        total_open_ml_used = 0.0
        total_closed_opened = 0
        
        for index, row in stock_rows.iterrows():
            if remaining_to_consume <= 0:
                break
                
            s_id = row['stock_id']
            r_open_ml = float(row['open_ml'])
            r_closed = int(row['closed_bottles'])
            r_open = int(row['open_bottles'])
            r_ml_per_bottle = float(row['ml_per_bottle'])
            r_total_ml = float(row['total_ml_available'])
            
            consumed_from_row = min(remaining_to_consume, r_total_ml)
            
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
            'closed_bottles_opened': total_closed_opened
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

def manage_locations():
    st.title("📍 Manage Locations")
    st.info("Dynamically add or remove locations. You cannot delete a location if it currently holds stock.")
    
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
                    st.success(f"Added location: {clean_name}")
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.error("This location name already exists.")
                finally:
                    conn.close()
            else:
                st.error("Location name cannot be empty.")
                
    st.markdown("---")
    st.subheader("Active Locations")
    
    for loc in locations:
        col1, col2 = st.columns([3, 1])
        col1.write(f"🏢 **{loc}**")
        
        if col2.button("Delete", key=f"del_{loc}", use_container_width=True):
            # Check if stock exists
            stock_check = fetch_data("SELECT COUNT(*) as count FROM STOCK_TABLE WHERE location = ?", (loc,))
            if stock_check.iloc[0]['count'] > 0:
                st.error(f"Cannot delete '{loc}' because it has active stock entries. Please clear stock first.")
            else:
                run_query("DELETE FROM LOCATIONS_TABLE WHERE location_name = ?", (loc,))
                st.success(f"Deleted location: {loc}")
                st.rerun()

def dashboard():
    st.title("📊 Dashboard Metrics")
    
    locations = get_active_locations()
    stock_df = fetch_data("SELECT * FROM STOCK_TABLE")
    events_df = fetch_data("SELECT * FROM EVENT_TABLE")
    
    if stock_df.empty:
        st.info("No stock data available yet.")
        return
        
    # A. Overall Metrics
    st.markdown("### 🌍 Overall Inventory Summary")
    total_ml = stock_df['total_ml_available'].sum()
    total_bottles = stock_df['open_bottles'].sum() + stock_df['closed_bottles'].sum()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Overall Total Inventory (ML)", f"{total_ml:,.2f}")
    col2.metric("Overall Total Bottles", f"{total_bottles}")
    
    if not events_df.empty:
        top_brand_df = events_df.groupby('brand_name')['ml_consumed'].sum().reset_index()
        top_brand = top_brand_df.loc[top_brand_df['ml_consumed'].idxmax()]['brand_name']
        col3.metric("Most Consumed Brand (Overall)", top_brand)
    else:
        col3.metric("Most Consumed Brand", "N/A")

    st.markdown("---")
    
    # B. Location-wise Split
    st.markdown("### 🏢 Location-wise Split")
    
    for loc in locations:
        st.markdown(f"#### 📌 {loc}")
        loc_stock = stock_df[stock_df['location'] == loc]
        loc_events = events_df[events_df['location'] == loc] if not events_df.empty else pd.DataFrame()
        
        if loc_stock.empty:
            st.info(f"No stock data for {loc}.")
            continue
            
        loc_ml = loc_stock['total_ml_available'].sum()
        loc_bottles = loc_stock['open_bottles'].sum() + loc_stock['closed_bottles'].sum()
        loc_sealed = loc_stock['closed_bottles'].sum()
        loc_open = loc_stock['open_bottles'].sum()
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total ML", f"{loc_ml:,.2f}")
        c2.metric("Total Bottles", f"{loc_bottles}")
        c3.metric("Sealed Bottles", f"{loc_sealed}")
        c4.metric("Open Bottles", f"{loc_open}")
        
        # C. Charts per Location
        colA, colB = st.columns(2)
        
        with colA:
            brand_stock = loc_stock.groupby('brand_name')['total_ml_available'].sum().reset_index()
            fig_pie = px.pie(brand_stock, names='brand_name', values='total_ml_available', hole=0.3, title="Stock by Brand")
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with colB:
            if not loc_events.empty:
                brand_cons = loc_events.groupby('brand_name')['ml_consumed'].sum().reset_index()
                fig_bar = px.bar(brand_cons, x='brand_name', y='ml_consumed', text_auto=True, title="Consumption by Brand")
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.write("No consumption data yet for this location.")
                
        st.markdown("<br>", unsafe_allow_html=True)


def add_stock():
    st.title("➕ Add Stock")
    
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
        bottle_count = st.number_input("Bottle Count *", min_value=1, value=1, step=1)
        
    with col2:
        ml_per_bottle = st.number_input("ML per Bottle *", min_value=1.0, value=750.0, step=50.0)
        supplier = st.text_input("Supplier")
        remarks = st.text_area("Remarks")
        
    if st.button("Save Stock", type="primary"):
        if not brand_name or not actual_item:
            st.error("Item and Brand Name are required.")
        else:
            open_bottles = 0
            closed_bottles = bottle_count
            open_ml = 0.0
            total_ml_available = bottle_count * ml_per_bottle
            
            run_query("INSERT OR IGNORE INTO BRAND_MASTER (brand_name, standard_ml, category) VALUES (?, ?, ?)", 
                      (brand_name.strip(), ml_per_bottle, actual_item.strip()))
            
            query = '''
                INSERT INTO STOCK_TABLE 
                (date_added, brand_name, item_name, ml_per_bottle, quantity_added, open_bottles, 
                 closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks, location)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            success = run_query(query, (
                date_added.strftime("%Y-%m-%d"), brand_name.strip(), actual_item.strip(), ml_per_bottle, 
                bottle_count, open_bottles, closed_bottles, open_ml, total_ml_available, 
                "", price, supplier.strip(), remarks.strip(), location
            ))
            if success:
                st.success(f"Successfully added {bottle_count} bottles of {brand_name} ({actual_item}) to {location}.")

def request_stock_addition():
    st.title("📝 Request Stock Addition")
    st.info("Submit a request for new stock. An Admin will review and approve it.")
    
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
        
    if st.button("Send Request", type="primary"):
        if not brand_name or not actual_item:
            st.error("Item and Brand Name are required.")
        else:
            query = '''
                INSERT INTO PENDING_STOCK_TABLE 
                (item, brand_name, bottle_count, ml_per_bottle, price, supplier, remarks, status, requested_by, date, location)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            success = run_query(query, (
                actual_item.strip(), brand_name.strip(), bottle_count, ml_per_bottle, price, 
                supplier.strip(), remarks.strip(), 'pending', 'Normal User', date_added.strftime("%Y-%m-%d"), location
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
                         closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks, location)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    success = run_query(stock_query, (
                        row['date'], row['brand_name'].strip(), row['item'].strip(), row['ml_per_bottle'], 
                        row['bottle_count'], open_bottles, closed_bottles, open_ml, total_ml_available, 
                        "", row['price'], row['supplier'], row['remarks'], row['location']
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

def view_stock():
    st.title("📦 View Stock")
    
    df = fetch_data("SELECT * FROM STOCK_TABLE ORDER BY date_added DESC")
    locations = ["All Locations"] + get_active_locations()
    
    if df.empty:
        st.info("No stock data found.")
        return
        
    col_loc, col_brand, col_search = st.columns(3)
    with col_loc:
        filter_loc = st.selectbox("Filter by Location", locations)
    with col_brand:
        brands = ["All"] + df['brand_name'].unique().tolist()
        filter_brand = st.selectbox("Filter by Brand", brands)
    with col_search:
        search = st.text_input("Search (Item, Supplier)")
        
    if filter_loc != "All Locations":
        df = df[df['location'] == filter_loc]
    if filter_brand != "All":
        df = df[df['brand_name'] == filter_brand]
    if search:
        search = search.lower()
        mask = df.astype(str).apply(lambda x: x.str.lower().str.contains(search)).any(axis=1)
        df = df[mask]
        
    df_display = rename_for_display(df)
    st.dataframe(df_display, use_container_width=True)
    
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df_display.to_excel(writer, index=False, sheet_name='Stock')
    st.download_button(label="📥 Download as Excel", data=buffer.getvalue(), file_name="stock_data.xlsx", mime="application/vnd.ms-excel")

def upload_stock_excel():
    st.title("📁 Upload Stock Excel")
    
    st.download_button(label="📥 Download Stock Template", 
                       data=get_template_excel('stock'), 
                       file_name="stock_template.xlsx", 
                       mime="application/vnd.ms-excel")
                           
    st.write("Upload an Excel file to bulk add stock. Required columns: `item`, `brand_name`, `location`, `ml_per_bottle`, `bottle_count`, `bill_no`, `date`, `price`, `supplier`, `remarks`")
    
    uploaded_file = st.file_uploader("Choose a .xlsx file (Stock)", type=["xlsx"])
    active_locations = get_active_locations()
    default_loc = active_locations[0] if active_locations else "Unknown"
    
    if uploaded_file is not None:
        try:
            df = pd.read_excel(uploaded_file)
            st.write("📄 File Preview:")
            st.dataframe(df.head())
            
            if st.button("Import Stock Data", type="primary"):
                success_count = 0
                failed_count = 0
                for index, row in df.iterrows():
                    brand_name = str(row.get('brand_name', '')).strip()
                    if not brand_name or pd.isna(brand_name) or brand_name == 'nan':
                        failed_count += 1
                        continue
                        
                    # Handle location
                    loc_val = str(row.get('location', '')).strip()
                    if not loc_val or pd.isna(loc_val) or loc_val == 'nan' or loc_val not in active_locations:
                        location = default_loc
                    else:
                        location = loc_val

                    item_name = str(row.get('item', ''))
                    ml_per_bottle = float(row.get('ml_per_bottle', 750.0)) if pd.notna(row.get('ml_per_bottle')) else 750.0
                    quantity_added = int(row.get('bottle_count', 1)) if pd.notna(row.get('bottle_count')) else 1
                    bill_no = str(row.get('bill_no', ''))
                    date_val = row.get('date', datetime.today().strftime("%Y-%m-%d"))
                    price = float(row.get('price', 0.0)) if pd.notna(row.get('price')) else 0.0
                    supplier = str(row.get('supplier', ''))
                    remarks = str(row.get('remarks', ''))
                    
                    if pd.isna(date_val): date_val = datetime.today().strftime("%Y-%m-%d")
                    else: date_val = str(date_val)[:10]

                    open_bottles = 0
                    closed_bottles = quantity_added
                    open_ml = 0.0
                    total_ml_available = quantity_added * ml_per_bottle
                    
                    run_query("INSERT OR IGNORE INTO BRAND_MASTER (brand_name, standard_ml, category) VALUES (?, ?, ?)", 
                          (brand_name, ml_per_bottle, item_name))
                          
                    query = '''
                        INSERT INTO STOCK_TABLE 
                        (date_added, brand_name, item_name, ml_per_bottle, quantity_added, open_bottles, 
                         closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks, location)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    if run_query(query, (date_val, brand_name, item_name, ml_per_bottle, quantity_added, 
                                         open_bottles, closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks, location)):
                        success_count += 1
                    else:
                        failed_count += 1
                
                st.success(f"✅ Successfully imported {success_count} rows!")
                if failed_count > 0:
                    st.warning(f"⚠️ Failed to import {failed_count} rows. Please check data format.")
        except Exception as e:
            st.error(f"Error processing file: {e}")

def upload_event_excel():
    st.title("📁 Upload Event Excel")
    
    st.download_button(label="📥 Download Event Template", 
                       data=get_template_excel('event'), 
                       file_name="event_template.xlsx", 
                       mime="application/vnd.ms-excel")
                           
    st.write("Upload an Excel file to bulk add events. Required columns: `date`, `occasion`, `brand_name`, `location`, `bottles_consumed`, `extra_ml`, `permit_number`")
    
    uploaded_file = st.file_uploader("Choose a .xlsx file (Events)", type=["xlsx"])
    active_locations = get_active_locations()
    default_loc = active_locations[0] if active_locations else "Unknown"
    
    if uploaded_file is not None:
        try:
            df = pd.read_excel(uploaded_file)
            st.write("📄 File Preview:")
            st.dataframe(df.head())
            
            if st.button("Import Event Data", type="primary"):
                success_count = 0
                failed_count = 0
                
                for index, row in df.iterrows():
                    brand_name = str(row.get('brand_name', '')).strip()
                    
                    # Handle location
                    loc_val = str(row.get('location', '')).strip()
                    if not loc_val or pd.isna(loc_val) or loc_val == 'nan' or loc_val not in active_locations:
                        location = default_loc
                    else:
                        location = loc_val
                        
                    bottles_consumed = int(row.get('bottles_consumed', 0)) if pd.notna(row.get('bottles_consumed')) else 0
                    extra_ml = float(row.get('extra_ml', 0.0)) if pd.notna(row.get('extra_ml')) else 0.0
                    date_val = row.get('date', datetime.today().strftime("%Y-%m-%d"))
                    occasion = str(row.get('occasion', ''))
                    permit_number = str(row.get('permit_number', ''))
                    
                    if pd.isna(date_val): date_val = datetime.today().strftime("%Y-%m-%d")
                    else: date_val = str(date_val)[:10]

                    if not brand_name or brand_name == 'nan':
                        failed_count += 1
                        st.warning(f"Row {index + 2}: Skipped due to invalid brand.")
                        continue
                        
                    if bottles_consumed == 0 and extra_ml == 0:
                        failed_count += 1
                        st.warning(f"Row {index + 2}: Skipped due to zero consumption.")
                        continue
                        
                    brand_data = fetch_data("SELECT MAX(ml_per_bottle) as max_ml FROM STOCK_TABLE WHERE brand_name = ? AND location = ?", (brand_name, location))
                    if brand_data.empty or pd.isna(brand_data.iloc[0]['max_ml']):
                        failed_count += 1
                        st.error(f"Row {index + 2}: Brand '{brand_name}' not found in current stock for '{location}'.")
                        continue
                        
                    ml_per_bottle = brand_data.iloc[0]['max_ml']
                    
                    if extra_ml >= ml_per_bottle:
                        failed_count += 1
                        st.error(f"Row {index + 2}: Invalid extra_ml ({extra_ml}) is greater than standard bottle size ({ml_per_bottle}). Adjust bottles_consumed instead.")
                        continue

                    ml_consumed = (bottles_consumed * ml_per_bottle) + extra_ml
                        
                    success, result = apply_consumption_logic(brand_name, location, ml_consumed)
                    
                    if success:
                        stats = result
                        insert_q = '''
                            INSERT INTO EVENT_TABLE 
                            (date, occasion, brand_name, total_bottles_before, total_ml_before, ml_consumed, 
                             closed_bottles_opened, open_ml_used, total_bottles_after, total_ml_after, 
                             open_bottles_after, closed_bottles_after, permit_number, location)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        '''
                        run_query(insert_q, (
                            date_val, occasion, brand_name, stats['total_bottles_before'], stats['total_ml_before'], 
                            ml_consumed, stats['closed_bottles_opened'], stats['open_ml_used'], stats['total_bottles_after'], 
                            stats['total_ml_after'], stats['open_bottles_after'], stats['closed_bottles_after'], permit_number, location
                        ))
                        success_count += 1
                    else:
                        failed_count += 1
                        st.error(f"Row {index + 2}: {result}")
                        
                st.success(f"✅ Successfully processed {success_count} events!")
                if failed_count > 0:
                    st.warning(f"⚠️ Failed to process {failed_count} events.")
        except Exception as e:
            st.error(f"Error processing file: {e}")

def create_event():
    st.title("🎉 Create Event (Consumption)")
    
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
                         open_bottles_after, closed_bottles_after, permit_number, location)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    run_query(insert_q, (
                        date.strftime("%Y-%m-%d"), occasion, brand, stats['total_bottles_before'], stats['total_ml_before'], 
                        total_ml_consumed, stats['closed_bottles_opened'], stats['open_ml_used'], stats['total_bottles_after'], 
                        stats['total_ml_after'], stats['open_bottles_after'], stats['closed_bottles_after'], permit_number, location
                    ))
                    st.success("✅ Consumption recorded successfully!")
                else:
                    st.error(f"Error during consumption: {result}")

def event_history():
    st.title("📜 Event History")
    
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
        
    df_display = rename_for_display(df)
    st.dataframe(df_display, use_container_width=True)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df_display.to_excel(writer, index=False, sheet_name='Events')
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
        
    df['total_current_bottles'] = df['current_closed_bottles'] + df['current_open_bottles']
    
    summary_mapping = {
        'brand_name': 'Brand Name',
        'location': 'Location',
        'ml_per_bottle': 'ML per Bottle',
        'total_bottles_added': 'Total Bottles Added',
        'current_closed_bottles': 'Sealed Bottles',
        'current_open_bottles': 'Open Bottles',
        'total_current_bottles': 'Total Bottles Left',
        'current_open_ml': 'Open ML Left',
        'current_total_ml': 'Total ML Left'
    }
    df_display = df.rename(columns=summary_mapping)
    st.dataframe(df_display, use_container_width=True)

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
    
    # Initialize session state for Role-Based Access
    if 'user_role' not in st.session_state:
        st.session_state['user_role'] = None

    # LOGIN SCREEN
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

    # SIDEBAR NAVIGATION
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
            "Upload Event Excel",
            "Create Event", 
            "Event History", 
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
    
    # ROUTING
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
    elif choice == "Upload Event Excel":
        upload_event_excel()
    elif choice == "Create Event":
        create_event()
    elif choice == "Event History":
        event_history()
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
