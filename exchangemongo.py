#Mejorado
import streamlit as st
import pandas as pd
import numpy as np
import random
import networkx as nx
import datetime
from io import BytesIO
from pymongo import MongoClient
import uuid

# === MongoDB connection with fallback ===
@st.cache_resource
def get_mongo_collection():
    client = MongoClient(st.secrets["mongo"]["uri"])
    db = client["car_exchange"]
    return db["uploads"]

try:
    mongo_collection = get_mongo_collection()
except Exception as e:
    st.warning("⚠️ MongoDB not connected. Uploads will not be stored.")
    mongo_collection = None

# === Load Excel with Offers and Wants ===
def load_offer_want_excel(file):
    xls = pd.ExcelFile(file)
    offers = xls.parse(xls.sheet_names[0])
    wants = xls.parse(xls.sheet_names[1])

    offers = offers.dropna(subset=['MODELO', 'VERSION'])
    wants = wants.dropna(subset=['MODELO', 'VERSION'])

    offers['modelo'] = offers['MODELO'].str.upper()
    offers['version'] = offers['VERSION'].str.upper()
    offers['full_name'] = offers['modelo'] + " - " + offers['version']
    offers['precio'] = offers['PRECIO'] if 'PRECIO' in offers else np.random.randint(200_000, 600_000, size=len(offers))

    wants['modelo'] = wants['MODELO'].str.upper()
    wants['version'] = wants['VERSION'].str.upper()
    wants['full_name'] = wants['modelo'] + " - " + wants['version']
    wants['precio'] = wants['PRECIO'] if 'PRECIO' in wants else np.random.randint(200_000, 600_000, size=len(wants))

    offer_data = offers[['full_name', 'precio']].to_dict('records')
    want_data = wants[['full_name', 'precio']].to_dict('records')
    return offer_data, want_data

# === Save to MongoDB ===
def save_user_data_to_mongo(offers, wants):
    user_id = str(uuid.uuid4())
    mongo_collection.insert_one({
        "user_id": user_id,
        "offers": offers,
        "wants": wants,
        "uploaded_at": datetime.datetime.utcnow()
    })
    return user_id

# === Load all uploads ===
def load_all_requests_from_mongo():
    data = mongo_collection.find()
    requests = []
    for idx, entry in enumerate(data):
        requests.append({
            'id': idx,
            'offers': entry['offers'],
            'wants': entry['wants'],
            'created_at': entry.get('uploaded_at', datetime.datetime.utcnow()),
            'status': 'pending'
        })
    return requests

# === Build graph ===
def build_graph(requests):
    G = nx.DiGraph()
    for req in requests:
        G.add_node(req['id'])

    for req_a in requests:
        for req_b in requests:
            if req_a['id'] == req_b['id']:
                continue
            if any(o['full_name'].lower() == w['full_name'].lower() for o in req_a['offers'] for w in req_b['wants']):
                G.add_edge(req_a['id'], req_b['id'])

    return G

# === Hybrid cycle extraction with duplication prevention ===
def sample_cycles_hybrid(G, request_map, max_len=10):
    all_cycles = []
    used_nodes = set()
    used_offers = set()

    for sub_nodes in nx.connected_components(G.to_undirected()):
        subgraph = G.subgraph(sub_nodes).copy()
        n = len(subgraph.nodes)

        if n <= 5:
            simple = list(nx.simple_cycles(subgraph))
            for cycle in simple:
                if len(cycle) >= 3 and cycle[0] in subgraph.successors(cycle[-1]):
                    cycle.append(cycle[0])
                    if not any(p in used_nodes for p in cycle):
                        if not violates_offer_conflict(cycle, request_map, used_offers):
                            all_cycles.append(cycle)
                            used_nodes.update(cycle)

        elif n <= 20:
            cycles = list(nx.simple_cycles(subgraph))
            cycles.sort(key=len, reverse=True)
            for cycle in cycles:
                if len(cycle) <= max_len and not any(p in used_nodes for p in cycle):
                    cycle.append(cycle[0])
                    if not violates_offer_conflict(cycle, request_map, used_offers):
                        all_cycles.append(cycle)
                        used_nodes.update(cycle)

        else:
            for start in subgraph.nodes:
                stack = [(start, [start])]
                while stack:
                    node, path = stack.pop()
                    for neighbor in subgraph.successors(node):
                        if neighbor == start and len(path) >= 3:
                            cycle = path + [start]
                            if not any(p in used_nodes for p in cycle):
                                if not violates_offer_conflict(cycle, request_map, used_offers):
                                    all_cycles.append(cycle)
                                    used_nodes.update(cycle)
                            break
                        elif neighbor not in path and len(path) < max_len:
                            stack.append((neighbor, path + [neighbor]))
    return all_cycles

# === Offer conflict check ===
def violates_offer_conflict(cycle, request_map, used_offers):
    for i in range(len(cycle) - 1):
        giver_id = cycle[i]
        receiver_id = cycle[i + 1]
        giver = request_map[giver_id]
        receiver = request_map[receiver_id]
        for offer in giver['offers']:
            for want in receiver['wants']:
                if offer['full_name'].lower() == want['full_name'].lower():
                    key = (giver_id, offer['full_name'])
                    if key in used_offers:
                        return True
                    used_offers.add(key)
    return False

# === Describe cycles ===
def describe_cycles(cycles, request_map):
    all_cycles = []
    user_cycles = []

    for cycle_id, cycle in enumerate(cycles):
        if len(cycle) < 3 or cycle[0] != cycle[-1]:
            continue

        description = []
        for i in range(len(cycle) - 1):
            giver_id = cycle[i]
            receiver_id = cycle[i + 1]
            giver = request_map[giver_id]
            receiver = request_map[receiver_id]
            matching_offer = next((o for o in giver['offers'] for w in receiver['wants']
                                   if o['full_name'].lower() == w['full_name'].lower()), None)
            if matching_offer:
                line = f"Participant {giver_id} offers '{matching_offer['full_name']}' → to Participant {receiver_id}"
                description.append(line)

        exchange_text = "\n".join(description)
        all_cycles.append({'cycle_id': cycle_id, 'exchange_path': exchange_text})

        if 0 in cycle:
            user_cycles.append({'cycle_id': cycle_id, 'exchange_path': exchange_text})

    return pd.DataFrame(all_cycles), pd.DataFrame(user_cycles)

st.title("🚗 Car Exchange Program - Multiuser Upload")

st.markdown("""
### Upload Your Offer/Wants File
Upload an Excel file with **two sheets**:
- Sheet 1: Your offers (with columns 'MODELO', 'VERSION', optional 'PRECIO')
- Sheet 2: Your wants (same format)
Your data will be added to the system and used in the next matching cycle.
""")

if mongo_collection is not None:
    user_file = st.file_uploader("📤 Upload Excel File (2 sheets)", type=['xlsx'])

    if user_file:
        offers, wants = load_offer_want_excel(user_file)
        save_user_data_to_mongo(offers, wants)
        st.success(f"✅ Upload successful. {len(offers)} offers and {len(wants)} wants saved.")
else:
    st.info("📂 Uploading is temporarily disabled because database is offline.")

st.markdown("---")
st.markdown("""
### Admin Only: Run Matching Now
This will use all stored uploads from all users and compute the current exchange cycles.
""")

if st.button("🧮 Run Matching Across All Uploads"):
    if mongo_collection is not None:
        all_requests = load_all_requests_from_mongo()
        request_map = {r['id']: r for r in all_requests}
        G = build_graph(all_requests)
        cycles = sample_cycles_hybrid(G, request_map)
        df_all, _ = describe_cycles(cycles, request_map)

        st.subheader("🔄 Preview of Exchange Cycles")
        st.dataframe(df_all.head(10))

        output = BytesIO()
        df_all.to_csv(output, index=False)
        st.download_button("📥 Download All Exchange Cycles", data=output.getvalue(), file_name="exchange_cycles.csv", mime="text/csv")
    else:
        st.error("❌ Database not connected. Cannot run matching.")

st.markdown("---")
with st.expander("⚠️ Admin Only: Danger Zone - Reset All Uploads"):
    password = st.text_input("Enter Admin Password to Reset:", type="password")
    if st.button("🗑️ Clear ALL uploaded data"):
        if password == "050699":
            mongo_collection.delete_many({})
            st.warning("All data has been deleted from MongoDB uploads collection.")
        else:
            st.error("❌ Incorrect password. Access denied.")
