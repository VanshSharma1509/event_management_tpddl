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
ADMIN_USER = "Amit Sharma"
ADMIN_PASS = "Ndpl@1234"

# ==========================================
# DATABASE SETUP & HELPERS
# ==========================================

DB_NAME = "liquor_inventory.db"

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    conn = get_connection()
    c = conn.cursor()
    
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
            remarks TEXT
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
            permit_number TEXT
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
            date TEXT
        )
    ''')
    
    # Safe migration for existing databases to add permit_number
    try:
        c.execute("ALTER TABLE EVENT_TABLE ADD COLUMN permit_number TEXT")
    except sqlite3.OperationalError:
        pass # Column already exists
        
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

def get_template_excel(template_type):
    buffer = io.BytesIO()
    if template_type == 'stock':
        df = pd.DataFrame(columns=['item', 'brand_name', 'ml_per_bottle', 'bottle_count', 'bill_no', 'date', 'price', 'supplier', 'remarks'])
    elif template_type == 'event':
        df = pd.DataFrame(columns=['date', 'occasion', 'brand_name', 'bottles_consumed', 'extra_ml', 'permit_number'])
    
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
        'requested_by': 'Requested By'
    }
    return df.rename(columns=mapping)

# ==========================================
# CORE REUSABLE LOGIC
# ==========================================

def apply_consumption_logic(brand, ml_consumed):
    if ml_consumed <= 0:
        return False, "Consumption amount must be greater than zero."

    conn = get_connection()
    c = conn.cursor()
    
    try:
        stock_rows = pd.read_sql_query(
            "SELECT * FROM STOCK_TABLE WHERE brand_name = ? AND total_ml_available > 0 ORDER BY date_added ASC", 
            conn, params=(brand,)
        )
        
        if stock_rows.empty:
            raise ValueError(f"No stock available for {brand}.")
            
        total_ml_before = float(stock_rows['total_ml_available'].sum())
        total_closed_before = int(stock_rows['closed_bottles'].sum())
        total_open_before = int(stock_rows['open_bottles'].sum())
        total_bottles_before = total_closed_before + total_open_before
        
        if ml_consumed > total_ml_before:
            raise ValueError(f"Insufficient stock for {brand}. Requested: {ml_consumed} ML, Available: {total_ml_before} ML.")
            
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
            "SELECT SUM(closed_bottles) as c, SUM(open_bottles) as o FROM STOCK_TABLE WHERE brand_name = ?", 
            conn, params=(brand,)
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

def dashboard():
    st.title("📊 Dashboard Metrics")
    
    stock_df = fetch_data("SELECT * FROM STOCK_TABLE")
    if stock_df.empty:
        st.info("No stock data available yet.")
        return
        
    total_ml = stock_df['total_ml_available'].sum()
    total_bottles = stock_df['open_bottles'].sum() + stock_df['closed_bottles'].sum()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Inventory (ML)", f"{total_ml:,.2f}")
    col2.metric("Total Bottles", f"{total_bottles}")
    
    events_df = fetch_data("SELECT brand_name, SUM(ml_consumed) as consumed FROM EVENT_TABLE GROUP BY brand_name ORDER BY consumed DESC")
    if not events_df.empty:
        top_brand = events_df.iloc[0]['brand_name']
        col3.metric("Most Consumed Brand", top_brand)
    else:
        col3.metric("Most Consumed Brand", "N/A")

    st.markdown("---")
    colA, colB = st.columns(2)
    
    with colA:
        st.subheader("Inventory by Brand (ML)")
        brand_stock = stock_df.groupby('brand_name')['total_ml_available'].sum().reset_index()
        fig_pie = px.pie(brand_stock, names='brand_name', values='total_ml_available', hole=0.3)
        st.plotly_chart(fig_pie, use_container_width=True)
        
    with colB:
        st.subheader("Low Stock Alert")
        low_stock = brand_stock[brand_stock['total_ml_available'] < 1000]
        if not low_stock.empty:
            st.warning("The following brands have less than 1000 ML remaining:")
            st.dataframe(low_stock.rename(columns={'brand_name':'Brand Name', 'total_ml_available': 'Total ML'}), hide_index=True)
        else:
            st.success("All brands have sufficient stock (>1000 ML).")
            
    if not events_df.empty:
        st.markdown("---")
        st.subheader("Total Consumption per Brand")
        fig_bar = px.bar(events_df, x='brand_name', y='consumed', text_auto=True, labels={'consumed':'Total ML Consumed', 'brand_name': 'Brand'})
        st.plotly_chart(fig_bar, use_container_width=True)


def add_stock():
    st.title("➕ Add Stock")
    
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
                 closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            success = run_query(query, (
                date_added.strftime("%Y-%m-%d"), brand_name.strip(), actual_item.strip(), ml_per_bottle, 
                bottle_count, open_bottles, closed_bottles, open_ml, total_ml_available, 
                "", price, supplier.strip(), remarks.strip()
            ))
            if success:
                st.success(f"Successfully added {bottle_count} bottles of {brand_name} ({actual_item}).")

def request_stock_addition():
    st.title("📝 Request Stock Addition")
    st.info("Submit a request for new stock. An Admin will review and approve it.")
    
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
                (item, brand_name, bottle_count, ml_per_bottle, price, supplier, remarks, status, requested_by, date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            success = run_query(query, (
                actual_item.strip(), brand_name.strip(), bottle_count, ml_per_bottle, price, 
                supplier.strip(), remarks.strip(), 'pending', 'Normal User', date_added.strftime("%Y-%m-%d")
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
        with st.expander(f"Request #{row['request_id']} | {row['bottle_count']}x {row['brand_name']} ({row['item']})", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
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
                    # Apply stock logic
                    open_bottles = 0
                    closed_bottles = row['bottle_count']
                    open_ml = 0.0
                    total_ml_available = row['bottle_count'] * row['ml_per_bottle']
                    
                    run_query("INSERT OR IGNORE INTO BRAND_MASTER (brand_name, standard_ml, category) VALUES (?, ?, ?)", 
                              (row['brand_name'].strip(), row['ml_per_bottle'], row['item'].strip()))
                    
                    stock_query = '''
                        INSERT INTO STOCK_TABLE 
                        (date_added, brand_name, item_name, ml_per_bottle, quantity_added, open_bottles, 
                         closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    success = run_query(stock_query, (
                        row['date'], row['brand_name'].strip(), row['item'].strip(), row['ml_per_bottle'], 
                        row['bottle_count'], open_bottles, closed_bottles, open_ml, total_ml_available, 
                        "", row['price'], row['supplier'], row['remarks']
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
    
    if df.empty:
        st.info("No stock data found.")
        return
        
    col1, col2 = st.columns(2)
    with col1:
        brands = ["All"] + df['brand_name'].unique().tolist()
        filter_brand = st.selectbox("Filter by Brand", brands)
    with col2:
        search = st.text_input("Search (Item, Supplier)")
        
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
                           
    st.write("Upload an Excel file to bulk add stock. Required columns: `item`, `brand_name`, `ml_per_bottle`, `bottle_count`, `bill_no`, `date`, `price`, `supplier`, `remarks`")
    
    uploaded_file = st.file_uploader("Choose a .xlsx file (Stock)", type=["xlsx"])
    
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
                         closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    if run_query(query, (date_val, brand_name, item_name, ml_per_bottle, quantity_added, 
                                         open_bottles, closed_bottles, open_ml, total_ml_available, bill_no, price, supplier, remarks)):
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
                           
    st.write("Upload an Excel file to bulk add events. Required columns: `date`, `occasion`, `brand_name`, `bottles_consumed`, `extra_ml`, `permit_number`")
    
    uploaded_file = st.file_uploader("Choose a .xlsx file (Events)", type=["xlsx"])
    
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
                        
                    brand_data = fetch_data("SELECT MAX(ml_per_bottle) as max_ml FROM STOCK_TABLE WHERE brand_name = ?", (brand_name,))
                    if brand_data.empty or pd.isna(brand_data.iloc[0]['max_ml']):
                        failed_count += 1
                        st.error(f"Row {index + 2}: Brand '{brand_name}' not found in current stock database.")
                        continue
                        
                    ml_per_bottle = brand_data.iloc[0]['max_ml']
                    
                    if extra_ml >= ml_per_bottle:
                        failed_count += 1
                        st.error(f"Row {index + 2}: Invalid extra_ml ({extra_ml}) is greater than standard bottle size ({ml_per_bottle}). Adjust bottles_consumed instead.")
                        continue

                    ml_consumed = (bottles_consumed * ml_per_bottle) + extra_ml
                        
                    success, result = apply_consumption_logic(brand_name, ml_consumed)
                    
                    if success:
                        stats = result
                        insert_q = '''
                            INSERT INTO EVENT_TABLE 
                            (date, occasion, brand_name, total_bottles_before, total_ml_before, ml_consumed, 
                             closed_bottles_opened, open_ml_used, total_bottles_after, total_ml_after, 
                             open_bottles_after, closed_bottles_after, permit_number)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        '''
                        run_query(insert_q, (
                            date_val, occasion, brand_name, stats['total_bottles_before'], stats['total_ml_before'], 
                            ml_consumed, stats['closed_bottles_opened'], stats['open_ml_used'], stats['total_bottles_after'], 
                            stats['total_ml_after'], stats['open_bottles_after'], stats['closed_bottles_after'], permit_number
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
    
    brands_df = fetch_data("SELECT DISTINCT brand_name FROM STOCK_TABLE WHERE total_ml_available > 0")
    if brands_df.empty:
        st.warning("No available stock to consume.")
        return
        
    brand = st.selectbox("Select Brand to Consume", brands_df['brand_name'].tolist())
    
    stock_rows = fetch_data("SELECT SUM(closed_bottles) as c, SUM(open_bottles) as o, SUM(total_ml_available) as t, MAX(ml_per_bottle) as m FROM STOCK_TABLE WHERE brand_name = ? AND total_ml_available > 0", (brand,))
    
    if not stock_rows.empty and stock_rows.iloc[0]['t'] is not None:
        total_ml_before = stock_rows.iloc[0]['t']
        total_closed_before = stock_rows.iloc[0]['c']
        total_open_before = stock_rows.iloc[0]['o']
        ml_per_bottle = stock_rows.iloc[0]['m']
        total_bottles_before = total_closed_before + total_open_before
        
        st.info(f"📦 **Current Stock available:** {total_bottles_before} Bottles ({total_closed_before} Sealed, {total_open_before} Open) | {total_ml_before:,.2f} Total ML")
        
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
                success, result = apply_consumption_logic(brand, total_ml_consumed)
                
                if success:
                    stats = result
                    insert_q = '''
                        INSERT INTO EVENT_TABLE 
                        (date, occasion, brand_name, total_bottles_before, total_ml_before, ml_consumed, 
                         closed_bottles_opened, open_ml_used, total_bottles_after, total_ml_after, 
                         open_bottles_after, closed_bottles_after, permit_number)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    run_query(insert_q, (
                        date.strftime("%Y-%m-%d"), occasion, brand, stats['total_bottles_before'], stats['total_ml_before'], 
                        total_ml_consumed, stats['closed_bottles_opened'], stats['open_ml_used'], stats['total_bottles_after'], 
                        stats['total_ml_after'], stats['open_bottles_after'], stats['closed_bottles_after'], permit_number
                    ))
                    st.success("✅ Consumption recorded successfully!")
                else:
                    st.error(f"Error during consumption: {result}")

def event_history():
    st.title("📜 Event History")
    
    df = fetch_data("SELECT * FROM EVENT_TABLE ORDER BY event_id DESC")
    
    if df.empty:
        st.info("No events recorded yet.")
        return
        
    col1, col2 = st.columns(2)
    with col1:
        brands = ["All"] + df['brand_name'].unique().tolist()
        filter_brand = st.selectbox("Filter by Brand", brands, key="ev_brand")
    with col2:
        occasions = ["All"] + df['occasion'].unique().tolist()
        filter_occ = st.selectbox("Filter by Occasion", occasions)
        
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
    
    query = '''
        SELECT 
            brand_name, 
            MAX(ml_per_bottle) as ml_per_bottle,
            SUM(quantity_added) as total_bottles_added,
            SUM(closed_bottles) as current_closed_bottles,
            SUM(open_bottles) as current_open_bottles,
            SUM(open_ml) as current_open_ml,
            SUM(total_ml_available) as current_total_ml
        FROM STOCK_TABLE
        GROUP BY brand_name
    '''
    df = fetch_data(query)
    
    if df.empty:
        st.info("No data available.")
        return
        
    df['total_current_bottles'] = df['current_closed_bottles'] + df['current_open_bottles']
    
    summary_mapping = {
        'brand_name': 'Brand Name',
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
    st.warning("⚠️ Edits here modify the raw database tables. Be careful.")
    
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
            st.write("Full access to manage inventory, events, and approve requests.")
            admin_user = st.text_input("Username")
            admin_pass = st.text_input("Password", type="password")
            if st.button("Login as Admin", type="primary"):
                if admin_user == ADMIN_USER and admin_pass == ADMIN_PASS:
                    st.session_state['user_role'] = 'admin'
                    st.rerun()
                else:
                    st.error("Invalid Admin Credentials")
        return # Stop execution until a role is selected

    # SIDEBAR NAVIGATION
    st.sidebar.title("🍷 Liquor Tracker")
    st.sidebar.write(f"Logged in as: **{st.session_state['user_role'].title()}**")
    
    if st.sidebar.button("Logout"):
        st.session_state['user_role'] = None
        st.rerun()
        
    st.sidebar.markdown("---")
    
    if st.session_state['user_role'] == 'admin':
        # Admin Notification for pending requests
        pending_count_df = fetch_data("SELECT COUNT(*) as count FROM PENDING_STOCK_TABLE WHERE status='pending'")
        if not pending_count_df.empty and pending_count_df.iloc[0]['count'] > 0:
            count = pending_count_df.iloc[0]['count']
            st.sidebar.warning(f"🔔 You have {count} pending stock requests")
            
        menu = [
            "Dashboard", 
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