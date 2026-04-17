import random
import csv
from pathlib import Path

random.seed(42)

COMPANY_BASE_NAMES = [
    "Acme", "Globex", "Initech", "Umbrella", "Wayne", "Stark", "Wonka",
    "Soylent", "Hooli", "Vehement", "Massive Dynamic", "Cyberdyne",
    "Tyrell", "Pied Piper", "Aperture", "Monarch", "LexCorp", "Oscorp",
    "Gringotts", "Blue Sun", "Initrode", "Virtucon", "Abstergo"
]

SUFFIXES = ["Corporation", "Corp", "Inc", "Limited", "Ltd", "LLC"]
CITIES = ["New York", "San Francisco", "Chicago", "Austin", "Seattle", "Boston", "Dallas"]
STREETS = ["Main Street", "Oak Road", "Pine Avenue", "Maple Drive", "Elm Street", "Cedar Boulevard"]
SUPPLIERS = ["Globex", "Initech", "Initrode", "Massive Dynamic", "Blue Sun", "Vehement"]
CUSTOMERS = ["RetailCo", "BuildIt", "MegaMart", "SupplyHub", "UrbanWorks", "CoreTrade"]
ACTIVITY_PLACES = ["NY", "CA", "TX", "WA", "IL", "MA", "NJ"]


def vary_company_name(base_name: str) -> str:
    suffix = random.choice(SUFFIXES)
    patterns = [
        f"{base_name} {suffix}",
        f"{base_name.upper()} {suffix}",
        f"{base_name}, {suffix}",
        f"{base_name}",
        f"{base_name} {suffix}."
    ]
    return random.choice(patterns)


def vary_address(num: int, street: str, city: str) -> str:
    replacements = {
        "Street": ["Street", "St", "St."],
        "Road": ["Road", "Rd", "Rd."],
        "Avenue": ["Avenue", "Ave", "Ave."],
        "Drive": ["Drive", "Dr", "Dr."],
        "Boulevard": ["Boulevard", "Blvd", "Blvd."]
    }
    varied_street = street
    for full, variants in replacements.items():
        if full in varied_street:
            varied_street = varied_street.replace(full, random.choice(variants))
    patterns = [
        f"{num} {varied_street}, {city}",
        f"{num} {varied_street} {city}",
        f"{num}, {varied_street}, {city}",
        f"{num} {varied_street}, {city}, USA"
    ]
    return random.choice(patterns)


def pick_pipe_list(values, min_items=1, max_items=3):
    k = random.randint(min_items, max_items)
    return "|".join(random.sample(values, k))


def build_master_entities(n=1600):
    entities = []
    for i in range(n):
        base = random.choice(COMPANY_BASE_NAMES) + f" {i}"
        street_num = random.randint(10, 999)
        street = random.choice(STREETS)
        city = random.choice(CITIES)
        revenue = round(random.uniform(100_000, 10_000_000), 2)
        margin = random.uniform(0.02, 0.25)
        profit = round(revenue * margin, 2)

        entities.append({
            "entity_key": f"corp_{i}",
            "base_name": base,
            "street_num": street_num,
            "street": street,
            "city": city,
            "revenue": revenue,
            "profit": profit,
        })
    return entities


def make_source1_rows(master_entities, count=1200):
    chosen = random.sample(master_entities, count)
    rows = []
    for e in chosen:
        rows.append({
            "corporate_name_s1": vary_company_name(e["base_name"]),
            "address": vary_address(e["street_num"], e["street"], e["city"]),
            "activity_places": pick_pipe_list(ACTIVITY_PLACES, 1, 3),
            "top_suppliers": pick_pipe_list(SUPPLIERS, 1, 4),
        })

    # Inject one bad row for tests
    #rows[0]["corporate_name_s1"] = ""
    return rows, chosen


def make_source2_rows(master_entities, overlap_entities, count=1200, overlap_count=320):
    overlap = random.sample(overlap_entities, overlap_count)
    remaining_needed = count - overlap_count

    remaining_pool = [e for e in master_entities if e not in overlap_entities]
    if remaining_needed > len(remaining_pool):
        raise ValueError(
            f"Not enough unique entities for source2 non-overlap. "
            f"Needed={remaining_needed}, available={len(remaining_pool)}."
        )
    extra = random.sample(remaining_pool, remaining_needed)

    chosen = overlap + extra
    rows = []

    for e in chosen:
        revenue = e["revenue"]
        profit = e["profit"]

        # Add some conflicting finance variation for overlaps
        if e in overlap and random.random() < 0.15:
            revenue = round(revenue * random.uniform(0.9, 1.1), 2)
            profit = round(profit * random.uniform(0.9, 1.1), 2)

        rows.append({
            "corporate_name_s2": vary_company_name(e["base_name"]),
            "main_customers": pick_pipe_list(CUSTOMERS, 1, 4),
            "revenue": revenue,
            "profit": profit,
        })

    # Inject a negative revenue row for quality testing
    #rows[1]["revenue"] = -1000
    return rows


def write_csv(rows, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    master = build_master_entities(2500)
    source1_rows, source1_entities = make_source1_rows(master, 1200)
    source2_rows = make_source2_rows(master, source1_entities, 1200, 320)

    write_csv(source1_rows, "data/sample_source1.csv")
    write_csv(source2_rows, "data/sample_source2.csv")

    print("Generated:")
    print("  data/sample_source1.csv")
    print("  data/sample_source2.csv")


if __name__ == "__main__":
    main()
