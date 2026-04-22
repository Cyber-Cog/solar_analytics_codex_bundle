# KPI Formulas Reference

Please confirm whether these definitions match your intent. If any formula is wrong, tell me the correct one and I’ll update the code.

---

## 1. Performance Ratio (PR)

**Current implementation (plant-level):**

- **PR = (Actual AC generation, kWh) / ((Plant capacity, kWp × Insolation(GTI), kWh/m²) /no. of days )× 100**
- Insolation(GTI) = sum of GTI over the period, converted to kWh/m² (for 15‑min data: `SUM(Irradiance) × 0.25 / 1000`).

**You said:** PR = generation / (dc capacity × no. of days) in % form.

- If you want **PR = Generation / (DC capacity × No. of days) × 100** (with generation and denominator in consistent units), say so and we can switch to this. - No, written above insolation is also there.

---

## 2. Plant Load Factor (PLF)

**Current implementation:**

- **PLF = (Total generation, kWh) / (Capacity, kW × 12 sun-hours × No. of days) × 100**

So: possible energy = Capacity_kW × 12 × days (kWh); PLF = actual / possible × 100.

**You said:** PLF = generation / (dc × 12 × no. of days).

- This is what is implemented. Confirm if “12” is sun-hours per day and capacity is in kW. -yes 

---

## 3. Energy Export / Net Generation

- **Energy export** = total AC energy (kWh) in the selected period (sum of inverter AC energy).
- **Net generation** = same as energy export in the current setup. - NO, it's energy export-energy Import
Add one more KPI card- energy Import next to Export which will take import value of the day. Import values will be less so could be in kWh
- Both are also exposed in **MWh** for display when values are large.
- Either we will have Energy export number of each inverter uploaded by metadata. If that's unavalable then sum(AC Power(kW) of all Inverters)/60000 in MWh

---

## 4. Export (if different from above)

- If “Export” has a different definition (e.g. meter export, or export after auxiliary consumption), specify the formula and data source. -- yes, energy export and export is same.

---

## 5. Generation loss (e.g. conversion loss)

- **Conversion loss** = DC energy − AC energy (inverter loss).
- In Faults → Inverter Efficiency, “Conv. Loss Energy” = total DC MWh − total AC MWh.
- If you use another definition (e.g. only certain losses), describe it.
**Losses due to disconneceted strings** = This should compare yield of the best performing SCB at that time and then divide it by no. of strings to get per String Yield of that SCB so that is the egneration loss of that particular string. In case of multiple string failiure in the same SCB just multiply ut by no of strings. 

---

## 6. Inverter-level PR (Analytics / Dashboard table)

**Current implementation:**

- **Yield** = Inverter energy (kWh) / Inverter DC capacity (kWp)/No. of Days → kWh/kWp.
- **PR** = (Yield, kWh/kWp) / (Insolation, kWh/m²) × 100 = (Yield / Insolation/No. of Days) × 100.
- Insolation = period GTI sum in kWh/m² (same as plant-level).

Confirm if this is correct or if you want a different formula (e.g. using number of days). use number of days, I have corrected the formula.

---

## 7. Target (daily or period)

- **Target (current)** = Capacity_kW × 4.5 (sun-hours per day) × No. of days, in kWh.
- Used for the energy chart and previously for an older PR variant. If you want target defined differently (e.g. from P50 or contract), share the formula.

No, understand that this target generation of the current day, so, it will be filled by the user once an dremain unchanged untill changed again. the graph would be based on the timeline which user will give like 06 to 18:00 Hrs, that time line will only be shown approx for the graph to be filled. which would be calulated per min = Energy target for the month(given by user)/No. of days in the month/(generation hrs i.e end time-start time of generation) you can use MWh accordingly if the values are high. 

---

## 8. CUF (Capacity Utilization Factor)

- **CUF** = (Energy generated in period, kWh) / (Capacity_kW × 24 × No. of days) × 100.
- Not currently shown; can be added if you need it. 

The main difference between CUF and PLF is that CUF has AC capacity and PLF has DC capacity nothing else, so just replace dc capacity with ac and it's your CUF, add days also.

---

## 9. Other parameters

- **Peak power**: max of sum of inverter AC power over the period -for  a particlar time stamp(per min)
- **Active power**: average of sum of inverter AC power over the period. okay
- **WMS GHI/GTI**: insolation in kWh/m² from WMS (sum of 15‑min values × 0.25 / 1000). I think WMS values are per minute so do it like - Irradiance/60000 to make it in kwh/m2

If you want to add or change any parameter (e.g. availability, specific loss breakdowns), describe the formula and I’ll align the code and this reference.
Yes add Total PA and GA. So, we will check whether how many strings were running*running time(you can calulate it by total number of strings * total generation hours - Strings breakdown * their downtime)/total number of strings *totaal generation hours for PA % and for GA % t0tal generation hours - how much time all strings were down/total generation hours
Give one more KPI card in dashboard i.e. Total Generation Loss(if not there)