#Versión final
#Password for all uploads reset: 050699
import streamlit as st
import pandas as pd
import numpy as np
import random
import networkx as nx
import datetime
from io import BytesIO
from pymongo import MongoClient
import uuid

@st.cache_resource
def get_mongo_collection():
    from pymongo import MongoClient
    client = MongoClient(st.secrets["mongo"]["uri"])
    db = client.car_exchange
    collection = db.user_uploads
    return collection

mongo_collection = get_mongo_collection() if "mongo" in st.secrets else None

def load_offer_want_excel(file):
    xls = pd.ExcelFile(file)
    offers = pd.read_excel(xls, 'Offers')
    wants = pd.read_excel(xls, 'Wants')
    return offers.to_dict('records'), wants.to_dict('records')

def save_user_data_to_mongo(offers, wants, name, agency_id):
    mongo_collection.update_one(
        {"agency_id": agency_id},
        {
            "$push": {
                "uploads": {
                    "offers": offers,
                    "wants": wants,
                    "uploaded_at": datetime.datetime.now()
                }
            },
            "$setOnInsert": {
                "user_id": str(uuid.uuid4()),
                "name": name,
                "agency_id": agency_id
            }
        },
        upsert=True
    )

st.markdown("---")
st.header("🔄 Run Matching Across All Uploads")

if st.button("Run Matching"):
    requests = []
    participants = mongo_collection.find({})
    for user in participants:
        for upload in user.get("uploads", []):
            requests.append({
                'id': user['agency_id'],  # or user['user_id'] if you want
                'offers': upload.get('offers', []),
                'wants': upload.get('wants', []),
                'created_at': upload.get('uploaded_at', datetime.datetime.now()),
                'status': 'pending'
            })
            
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

st.title("🚗 Upload Your Car Exchange Data")

if mongo_collection is None:
    st.error("MongoDB is not configured. Please check your secrets.")
    st.stop()

st.info("Please upload your Excel file with two sheets: 'Offers' and 'Wants'.")

name = st.text_input("Enter your Name")
agency_id = st.text_input("Enter your Agency ID")

user_file = st.file_uploader("📤 Upload your Excel file:", type=['xlsx'])

if st.button("Upload File"):
    if not name.strip() or not agency_id.strip():
        st.error("Please fill both your Name and your Agency ID before uploading.")
    elif not user_file:
        st.error("Please select an Excel file to upload.")
    else:
        offers, wants = load_offer_want_excel(user_file)
        save_user_data_to_mongo(offers, wants, name, agency_id)
        st.success(f"✅ Upload successful! {len(offers)} offers and {len(wants)} wants saved.")
        st.balloons()
