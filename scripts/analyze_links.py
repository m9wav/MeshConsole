#!/usr/bin/env python3
"""Analyze route adjacencies to distinguish real RF links from hash collision artifacts."""
import sqlite3, json, math

def hav(a, b):
    R = 6371
    dl = math.radians(b[0]-a[0]); dn = math.radians(b[1]-a[1])
    x = math.sin(dl/2)**2 + math.cos(math.radians(a[0]))*math.cos(math.radians(b[0]))*math.sin(dn/2)**2
    return R * 2 * math.atan2(math.sqrt(x), math.sqrt(1-x))

conn = sqlite3.connect("meshtastic_messages.db")
c = conn.cursor()

# Load coords
c.execute("SELECT from_id, raw_packet FROM packets WHERE port_name IN ('NODEINFO','NODEINFO_APP') AND backend='meshcore' ORDER BY timestamp DESC")
coords = {}
for fid, raw_str in c.fetchall():
    raw = json.loads(raw_str)
    name = raw.get('adv_name', '')
    lat = raw.get('adv_lat') or raw.get('latitude')
    lon = raw.get('adv_lon') or raw.get('longitude')
    if name and lat and lon and abs(float(lat)) > 0.01 and name not in coords:
        coords[name] = (float(lat), float(lon))

# Load adjacency
c.execute("SELECT node_hash, neighbor_hash, node_candidate, count FROM route_adjacency ORDER BY count DESC")
adj = c.fetchall()

# Best candidate per hash
hash_counts = {}
for nh, nbr, cand, cnt in adj:
    hash_counts.setdefault(nh, {})[cand] = hash_counts.get(nh, {}).get(cand, 0) + cnt
hash_best = {h: max(cands, key=cands.get) for h, cands in hash_counts.items()}

# Count direct adjacency from actual route paths
c.execute("SELECT raw_packet FROM packets WHERE backend='meshcore' AND raw_packet LIKE '%path%' ORDER BY timestamp DESC LIMIT 500")
pair_counts = {}
for (raw_str,) in c.fetchall():
    raw = json.loads(raw_str)
    path = raw.get('path', '')
    if not path or len(path) < 4:
        continue
    hs = raw.get('path_hash_size', 1) or 1
    step = hs * 2
    hashes = [path[i:i+step] for i in range(0, len(path), step)]
    for i in range(len(hashes)-1):
        pair = tuple(sorted([hashes[i], hashes[i+1]]))
        pair_counts[pair] = pair_counts.get(pair, 0) + 1

# Top 30 direct adjacencies
print("Top 30 DIRECT route adjacencies (from actual packet paths):")
print(f"{'count':>5}  {'dist':>6}  {'verdict':>8}  link")
print("-" * 80)
for pair, cnt in sorted(pair_counts.items(), key=lambda x: -x[1])[:30]:
    h1, h2 = pair
    n1 = hash_best.get(h1, h1)
    n2 = hash_best.get(h2, h2)
    c1 = coords.get(n1)
    c2 = coords.get(n2)
    if c1 and c2:
        dist = hav(c1, c2)
        verdict = "REAL" if dist < 80 else ("MAYBE" if dist < 120 else "FAKE")
        print(f"{cnt:>5}  {dist:>5.0f}km  {verdict:>8}  {n1} <-> {n2}")
    else:
        print(f"{cnt:>5}  {'?':>6}  {'?':>8}  {n1} <-> {n2}")

# Links 80-120km we're missing
print("\n\nHigh-traffic links 80-120km (potentially legit, currently filtered out):")
print(f"{'routes':>6}  {'dist':>5}  link")
print("-" * 60)
for pair, route_cnt in sorted(pair_counts.items(), key=lambda x: -x[1]):
    h1, h2 = pair
    n1 = hash_best.get(h1, h1)
    n2 = hash_best.get(h2, h2)
    c1 = coords.get(n1)
    c2 = coords.get(n2)
    if not c1 or not c2:
        continue
    dist = hav(c1, c2)
    if 80 <= dist <= 120 and route_cnt >= 5:
        print(f"{route_cnt:>6}  {dist:>4.0f}km  {n1} <-> {n2}")

conn.close()
