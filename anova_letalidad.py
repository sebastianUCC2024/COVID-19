# Ejecuta el script pasando la ruta del CSV como argumento.
import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from scipy import stats

parser = argparse.ArgumentParser()
parser.add_argument("csv", type=Path)
args = parser.parse_args()

# Usa una fecha de corte común y agrupa por el porcentaje de habitantes de 65 años o más.
cutoff = "2024-08-04"
groups = {
    "Joven (<8%)": [],
    "Intermedia (8% a <15%)": [],
    "Envejecida (>=15%)": [],
}
seen = set()
missing_age = 0

# Lee el CSV fila por fila. El corte común evita contar varias fechas del mismo lugar.
with args.csv.open(encoding="utf-8-sig", newline="") as source:
    for row in csv.DictReader(source):
        # Excluye las demás fechas y las filas sin casos positivos para poder calcular la tasa.
        if row["date"] != cutoff or float(row["total_cases"]) <= 0:
            continue
        # Comprueba que cada ubicación aparezca una sola vez en la fecha de corte.
        country = row["iso_code"]
        if country in seen:
            raise ValueError(f"Duplicate location on cutoff date: {country}")
        seen.add(country)
        # Excluye las ubicaciones sin dato demográfico y registra cuántas quedaron fuera.
        if not row["aged_65_older"].strip():
            missing_age += 1
            continue
        # Clasifica toda la ubicación según el porcentaje de población de 65 años o más.
        share_65 = float(row["aged_65_older"])
        if share_65 >= 15:
            group = "Envejecida (>=15%)"
        elif share_65 >= 8:
            group = "Intermedia (8% a <15%)"
        else:
            group = "Joven (<8%)"
        # Calcula las muertes acumuladas por cada 100 casos confirmados acumulados.
        lethality = 100 * float(row["total_deaths"]) / float(row["total_cases"])
        groups[group].append(lethality)

# Cada arreglo contiene una tasa por país o territorio. Todas las ubicaciones pesan igual.
samples = [np.array(values) for values in groups.values()]
# Un residuo es la tasa de la ubicación menos la media de su grupo.
residuals = np.concatenate([values - values.mean() for values in samples])
print(f"Corte: {cutoff}. Ubicaciones: {len(seen)}. Sin dato etario: {missing_age}")
# Resume el tamaño, la media, la mediana y la desviación estándar de cada grupo.
for name, values in zip(groups, samples):
    print(f"{name}: n={len(values)}, media={values.mean():.4f}%, "
          f"mediana={np.median(values):.4f}%, DE={values.std(ddof=1):.4f}")
# Shapiro-Wilk evalúa la normalidad de los residuos.
print("Shapiro-Wilk:", stats.shapiro(residuals))
# Levene evalúa si las varianzas son iguales entre grupos, usando la mediana como centro.
print("Levene (mediana):", stats.levene(*samples, center="median"))
# H0: las medias de letalidad de los tres grupos son iguales.
# H1: al menos una media difiere. El ANOVA clásico supone varianzas iguales.
print("ANOVA clasico:", stats.f_oneway(*samples))
# Welch también compara medias y permite varianzas distintas.
print("ANOVA de Welch:", stats.f_oneway(*samples, equal_var=False))
# Kruskal-Wallis compara los rangos entre grupos como comprobación complementaria.
print("Kruskal-Wallis:", stats.kruskal(*samples))

# Dibuja el 50 % central de cada grupo en una caja, con la mediana como línea interior.
# Los bigotes llegan a las observaciones dentro del límite de 1,5 rangos intercuartílicos.
fig, ax = plt.subplots(figsize=(9, 5.2), layout="constrained")
ax.boxplot(samples, showfliers=False, patch_artist=True,
           boxprops={"facecolor": "#d7e9f2"}, medianprops={"color": "#193c55", "linewidth": 2})
# Superpone todas las observaciones, incluidos los extremos. El desplazamiento horizontal
# reduce el solapamiento de puntos y la semilla 42 lo hace reproducible.
rng = np.random.default_rng(42)
for index, values in enumerate(samples, start=1):
    ax.scatter(index + rng.uniform(-0.14, 0.14, len(values)), values,
               s=17, alpha=0.55, color="#1d6386")
ax.set_xticks(range(1, len(groups) + 1),
              [f"{label}\n(n = {len(values)})" for label, values in groups.items()])
ax.set_title("Letalidad acumulada por estructura etaria\n4 de agosto de 2024")
ax.set_xlabel("Grupo según el porcentaje de población de 65 años o más")
ax.set_ylabel("Muertes por cada 100 casos confirmados")
ax.grid(axis="y", alpha=0.25)
# Guarda la imagen PNG en la misma carpeta que el script.
destination = Path(__file__).with_name("anova_letalidad.png")
fig.savefig(destination, dpi=180)
