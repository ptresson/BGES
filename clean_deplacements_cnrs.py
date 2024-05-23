# Alé, second round with a more consistent export sheet:
# make something GESLAB with it.

import pandas as pd
import dateparser as dp
import geopy as gp
import math
import pickle as pk
import re



# Parse dates
def parse(date):
    parsed = dp.parse(date)
    if parsed is not None:
        return parsed.strftime("%Y-%m-%d")
    else:
        raise ValueError(f"Cannot parse |{date}|.")


def dmy_parse(date):
    return dp.parse(date, settings={"DATE_ORDER": "DMY"}).strftime("%d/%m/%Y")


# Expected result.
lab = pd.DataFrame()
lab["mission"] = pd.Series(dtype=int)
lab["date"] = pd.Series(dtype=str)
lab["start"] = pd.Series(dtype=str)
lab["cstart"] = pd.Series(dtype=str)
lab["end"] = pd.Series(dtype=str)
lab["cend"] = pd.Series(dtype=str)
lab["mode"] = pd.Series(dtype=str)
lab["nb"] = pd.Series(dtype=int)
lab["trip"] = pd.Series(dtype=pd.CategoricalDtype(["OUI", "NON"]))

# Gather all this into one big table.
raw_columns_selection = dict(
    glab="Groupe labo",
    mission="Numéro mission",
    start_date="Date de départ",
    start_city="Ville de départ",
    start_country="Pays de départ",
    end_city="Ville de destination",
    end_country="Pays de destination",
    means="Moyens de transport",
    nb_in_car="Nb de pers. dans la voiture",
    round="Aller / Retour",
    reason="Motif du déplacement",
    status="Statut agent",
    return_date="Date de retour",
)

print("Reading raw data..")
# Retrieve all data into one big table.
raw = pd.DataFrame(columns=["table"] + list(raw_columns_selection.keys()))
for i in ("CNRS",):
    # for j in (2019, 2020):
    #  one year at a time
    for j in (2017,):
        table = f"{i}_{j}"
        
        if j == 2017:
            read = pd.read_csv(f"raw_{table}.csv", sep=";", encoding='ISO-8859-1',engine="python")
        else:
            read = pd.read_csv(f"raw_{table}.csv", sep=";", encoding='ISO-8859-1')

        sub = pd.DataFrame(
            {name: read[raw_name] for name, raw_name in raw_columns_selection.items()}
        )
        
        sub["table"] = table
        raw = raw.append(sub, ignore_index=True, verify_integrity=True)
raw.reindex()
print("Read.")
print(raw)

# TODO: reintegrate those weird lines?
# weird = (
#     26991,
#     27250,
#     27255,
#     26911,
#     27016,
#     27151,
#     27278,
#     27890,
#     28162,
#     28302,
#     28422,
#     28456,
# )
# # 2018
# weird = (
#     25838,
#     25802,
#     25839,
#     25840,
#     25925,
#     26534,
# )
# 2017
weird = (
    25327,
    25426,
    25190,
    24522,
    25614,
)


aside = pd.DataFrame()
for w in weird:
    aside = aside.append(raw.loc[raw.mission == w])
    raw = raw.loc[raw.mission != w]

# Export for manual editions.
for short, long in raw_columns_selection.items():
    aside[long] = aside[short]
    del aside[short]
with open("weird.tsv", "w") as file:
    file.write(aside.to_csv(sep="\t", index=False))

# Remove useless information
assert all(raw.glab == 656)
del raw["glab"]

# The "mission" column seems consistent.
lab["mission"] = raw.mission

# Parse all dates
print("Parsing dates..")
lab.date = raw.start_date.apply(dmy_parse)

# Parse cities.
print("Parsing cities..")
locator = gp.Nominatim(user_agent="gl")
try:
    with open("cache.pk", "rb") as file:
        cache = pk.load(file)
except:
    print("Cache not found, restarting..")
    cache = {}  # {search: geoloc}


def search(city, country):
    """Produce working search string, including special-cases."""
    fixes = [
        ("ST GEORGES D ORQUES", "Saint-Georges d'Orques"),
        ("Chennay", "Chennai"),
        ("Galgary", "Calgary"),
        ("sofia antipolis", "Sophia Antipolis"),
        ("Honk Kong", "Hong Kong"),
        ("Pouget, Mauguio", "Le Pouget"),
        ("Vérargues,Mauguio", "Vérargues"),
        ("16232 Athènes", "Athènes"),
        ("1900-385 LISBONNE", "Lisbonne"),
        ("Barcelonne", "Barcelone"),
        ("MONTFERRIEZ", "Montferrier-sur-Lez"),
        ("L-1359 Krichberg", "Krichberg"),
        ("Val de Nuria", "Serrat"),
        ("Aaren", "Aachen"),
        ("G61 3LN, Glasgow", "Glasgow"),
        ("STE CROIX DE QUINTILLARGU", "Sainte-Croix-de-Quintillargues"),
        ("Tronheim", "Trondheim"),
        ("SEYSSINET PARIZET", "Seyssins"),
        ("Biot Sophia Antipolis", "Sophia Antipolis"),
        ("Equilles", "Eguilles"),
        ("SOTO DE LLANERA", "Robledo"),
        ("Sait Etienne", "Saint-Etienne"),
        ("SAIT ETIENNE ", "Saint-Etienne"),
        ("SARREBRUKEN", "Sarrebruck"),
        ("ST ETIENNE DU ROUVAY", "Saint-Etienne du Rouvray"),
        ("CANEJEAN", "Canéjan"),
        ("Dagsthul", "Dagstuhl"),
        ("Melboune", "Melbourne"),
        ("St Gely/ Montpellier", "St-Gely du Fesc"),
        ("Saaint Drezery", "Saint-Drezery"),
        ("ST MARTIN D HERES", "Saint-Martin-d'Héres"),
        ("EQUILLES", "Eguilles"),
        ("Amsterdam ( escal", "Amsterdam"),
        ("Jyvanskyla","Jyväskylä"),
        ("Nycosie","Nicosie"),
        ("KREMS AN DER DONA","Krems an der Donau"),
        ("BRUXELLES 1050","Bruxelles"),
        ("Gennevillers","Gennevilliers"),
        ("KOSICH","Košice"),
        ("Peoria / Macomb","Peoria"),
        ("MAUGUIO VILLEN CASTELN","Mauguio"),
        ("COPENHAGUE DK-2200","Copenhague"),
        ("Thelassanique","Thessalonique"),
        ("Manaus/ Alter do chao","Manaus"),
        ("Sao Luis / Jericoacoara","São Luís"),
        ("Foz d?Iguazu","Foz do Iguaçu"),
        ("Sao Paulo14-15/9 +Bélem le 16/09", "São Paulo"),
        ("STUPCA","Słupca"),
        ("Sofia Antipolis","Sophia Antipolis"),
        ("1033 CHESNAUX-SUR-LAUSANN","Cheseaux-sur-Lausanne"),
        ("Marseille puis Corse","Ajaccio"),
        ("Fortalezza, Ceara","Fortaleza"),
        ("KOLN 50735","Köln"),
        ("Howald","Le Hohwald"),
        ("BELO HORIZONTE 31.340-052","Belo Horizonte"),
        ("BARCELONNE", "Barcelone"),
        ("Montpellier / St Martin Londres", "Montpellier"),
        ("Hannovre", "Hanovre"),
        ("Palma de Malloque", "Palma"),
        ("'s-Hertogenbosch 5211XC","'s-Hertogenbosch"),
        ("Poushchino, Moscow Region","Moscow"),
        ("Geronne","Gérone"),
        ("Allborg","Aalborg"),
        ("Bergen N5020","Bergen"),
        ("Begrade","Belgrade"),
        ("chandigars","Chandigarh"),
    ]
    for bad, good in fixes:
        if city.startswith(bad):
            city = good
            print(good)
            break
    # Remove cedex.
    city = city.split("CEDEX")[0]
    city = city.split("cedex")[0]
    city = city.split("Cedex")[0]
    city = city.split("convenance Personnelle")[0]
    city = city.split("convenance personnelle")[0]
    city = city.split("Convenance Personnelle")[0]
    city = city.split("convenance pers")[0]
    city = city.split("conv perso")[0]
    if type(country) is float and math.isnan(country):
        return city
    fixes = [
        ("Rép. Tchèque", "Republique Tcheque"),
        ("Féd. De Russie", "Russie"),
        ("Nvelle Zélande", "Nouvelle Zélande"),
        ("Ne pas utiliser", ""),
    ]
    for bad, good in fixes:
        if country.startswith(bad):
            country = good
            break
    if city == "Melbourne" and country == "Nouvelle Zélande":
        country = "Australie"
    if city == "Bruxelles" and country == "France":
        country = "Belgique"
    if city == "Thessalonique" and country == "France":
        country = "Grèce"
    if city == "New Dheli" and country == "France":
        country = "India"
    if city == "Köln" and country == "France":
        country = "Germany"
    if city == "Howald" and country == "France":
        country = "Luxembourg"
    if city == "'s-Hertogenbosch" and country == "France":
        country = "Netherlands"
    if city == "SOPHIA ANTIPOLIS" and country == "Danemark":
        country = "France"
    if city == "MELUN" and country == "Japon":
        city = "Tokyo" # just guessing

    return city + ", " + country


for n, (i, row) in enumerate(raw.iterrows()):
    print(f"{n} / {len(raw)}")
    for col in ("start", "end"):
        city, country = row[f"{col}_city"], row[f"{col}_country"]
        s = search(city, country)
        print(s, end="")
        cached = False
        g = cache.get(s)
        

        ####################### reformulation for python <3.8
        if g is None:
            g = locator.geocode(s, language='fr') # get french name
            if g is not None :
                loc = locator.reverse(g.point, language='fr') # get french name
                cache[s] = (g, loc)
            else:
                raise ValueError(f"\nCannot locate '{s}'.")
        else:
            g, loc = g
            print(" (cached)")
            cached = True
        address = loc.raw["address"]
        
        ####################### reformulation for python <3.8
        # try:
        #     attempts = ["city", "town", "village", "hamlet"]
        #     c = address.get(attempts.pop(0))
        #     print(c)
        #     if c is None:
        #         pass
        #     city = c
        attempts = ["city", "town", "village", "hamlet"]
        for attempt in attempts:
            # c = address.get(attempts.pop(0))
            c = address.get(attempt)
            
            if c is not None:
                print(c)
                city = c
                break
        # print(city)

        ####################### reformulation for python <3.8
        # except Exception as e:
        #     # This one should be alright.
        if country == "Barbade":
            city = "Bridgetown"
        elif city in ("Tunis", "Stanford"):
            pass
        elif any(s.startswith(p) for p in ("Athène", "athènes", "ATHENE")):
            city = "Athènes"
        # else:
        #     raise e


        country = address["country"]
        if not cached:
            print(f"\n{city}, {country}")
        lab.loc[i, f"{col}"] = city
        lab.loc[i, f"c{col}"] = country
        print()

with open("cache.pk", "wb") as file:
    pk.dump(cache, file)


# Parse modes.
# authorized = [
#     "Avion",
#     "Voiture personnelle",
#     "Train",
#     "Bus",
#     "Taxi",
#     "RER",
#     "Métro",
#     "Tramway",
#     "Ferry",
# ]
authorized = [
    "Avion",
    "Voiture",
    "Train",
    "Bus",
    "Taxi",
    "RER",
    "Métro",
    "Tramway",
    "Ferry",
]
regauth = [re.compile(p, flags=re.IGNORECASE) for p in authorized]

startw = r"(?:\b|(?<=_))"
endw = r"(?:\b|(?=_))"
wrap_word = lambda pattern: startw + pattern + endw

# patterns = {
#     "Voiture personnelle": [
#         re.compile(p, flags=re.IGNORECASE)
#         for p in [
#             wrap_word(r"voiture"),
#             wrap_word(r"voit"),
#             wrap_word(r"Passager"),
#             wrap_word(r"v[eé]hic(ule)?\d*"),
#             wrap_word(r"veh"),
#             wrap_word(r"location de v[eé]hic(ule)?\d*"),
#         ]
#     ],
#     "Métro": [
#         re.compile(p, flags=re.IGNORECASE)
#         for p in [
#             wrap_word(r"m[eé]tro"),
#         ]
#     ],
#     "Ferry": [
#         re.compile(p, flags=re.IGNORECASE)
#         for p in [
#             wrap_word(r"bateau"),
#         ]
#     ],
#     None: [
#         re.compile(p, flags=re.IGNORECASE)
#         for p in [
#             wrap_word(r"divers"),
#         ]
#     ],
# }

patterns = {
    "Voiture": [
        re.compile(p, flags=re.IGNORECASE)
        for p in [
            wrap_word(r"voiture"),
            wrap_word(r"voit"),
            wrap_word(r"Passager"),
            wrap_word(r"v[eé]hic(ule)?\d*"),
            wrap_word(r"veh"),
            wrap_word(r"location de v[eé]hic(ule)?\d*"),
        ]
    ],
    "Métro": [
        re.compile(p, flags=re.IGNORECASE)
        for p in [
            wrap_word(r"m[eé]tro"),
        ]
    ],
    "Ferry": [
        re.compile(p, flags=re.IGNORECASE)
        for p in [
            wrap_word(r"bateau"),
        ]
    ],
    None: [
        re.compile(p, flags=re.IGNORECASE)
        for p in [
            wrap_word(r"divers"),
        ]
    ],
}

def parse_one(mean):
    for i, p in enumerate(regauth):
        if p.match(mean):
            return authorized[i]
    for m, pats in patterns.items():
        for p in pats:
            if p.match(mean):
                return m
    raise ValueError(f"Cannot parse {mean}.")


for i, row in raw.iterrows():
    means = row["means"]
    print(means)
    if type(means) is float and math.isnan(means):
        print(f"Withdraw {row.mission}!")
        assert False
    if means not in authorized:
        while True:
            means = [parse_one(m.strip()) for m in means.split(",")]
            means = [m for m in means if m is not None]
            if not means:
                print(f"Withdraw {row.mission}!")
                assert False
            if len(pd.Series(means).unique()) == 1:
                means = means[0]
                if means in authorized:
                    break
            # Keep the most polluting.
            means, _ = max(
                [(m, authorized.index(m)) for m in means], key=lambda a: -a[1]
            )
            break
    lab.loc[i, "mode"] = means
    if not math.isnan(row.nb_in_car):
        lab.loc[i, "nb"] = row.nb_in_car

lab['mode'] = lab['mode'].str.lower() # get all means lowercase

# Nb of people in the cars.. trust'em, or 1 if not specified.
lab.nb = lab.nb.apply(lambda n: "1" if math.isnan(n) else str(int(n)))

# Round trip, trust them as well.
lab.trip = raw["round"]
lab['motif'] = raw["reason"]
lab['statut'] = raw["status"]

# Export for geslab.
with open("lab.tsv", "w") as file:
    file.write(lab.to_csv(sep="\t", index=False,
    header=["# mission",
    "Date de départ",
    "Ville de départ",
    "Pays de départ",
    "Ville de destination",
    "Pays de destination",
    "Mode de déplacement",
    "Nb de personnes dans la voiture",
    "Aller Retour (OUI si identiques, NON si différents)",
    "Motif du déplacement (optionnel)",
    "Statut de l'agent (optionnel)"]))
 