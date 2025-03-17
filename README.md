# 🚔 SF Crime Analysis Using PySpark  
**Author:** Kim Jin  
**Project Type:** Class Homework  
**Date:** Jun. 2024  
**Dataset:** [San Francisco Police Department Incident Reports](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-Historical-2003/tmnf-yvry/about_data)  

---

## Project Overview  
This project analyzes **San Francisco Police Department Incident Reports** using **PySpark** for large-scale data processing. The dataset contains **crime reports from 2003 to the present**, including **crime categories, locations, dates, times, and resolutions**.  

The goal is to **explore crime patterns across categories, districts, and time periods** to gain insights into **crime distribution, trends, and policy implications**.

---

## Dataset & Preprocessing  
- **Data Source:** Publicly available SF Police Incident Reports.  
- **Preprocessing Steps:**  
  - Loaded data using **PySpark** for efficient handling.  
  - **Inferred schema** automatically for structured querying.  
  - Handled missing values and **cleaned categorical inconsistencies**.  
  - Created **new temporal features** (year, month, hour) for trend analysis.  

---

## Research Questions & Methods  

### **1️. What are the most common crime categories in SF?**  
- **Method:** Aggregated **crime counts by category**.  
- **Visualization:** **Bar plot** showing crime frequency by type.  
- **Insights:**  
  - **Theft is the most common crime**, significantly outnumbering others.  
  - **Assault and non-criminal offenses** are also highly frequent.  

---

### **2️. How does crime distribution vary across districts?**  
- **Method:** Grouped crimes by **police district**.  
- **Visualization:** **Bar plot** displaying district-wise crime frequency.  
- **Insights:**  
  - **Southern SF reports the most crimes**, while **Richmond appears safer**.  
  - A **missing (NaN) district** suggests data quality issues.  

---

### **3️. How does crime frequency vary on Sundays in Downtown SF?**  
- **Method:** Filtered **Sunday incidents within SF downtown coordinates**.  
- **Visualization:** **Time series plot** tracking Sunday crime trends.  
- **Insights:**  
  - **Crime rates slightly increase over time on Sundays**.  
  - **Mid-year spikes** suggest connections to **holidays or city events**.  
  - **2013 anomaly** possibly linked to the **BART strike or Asiana plane crash**.  

---

### **4️. Monthly crime trends from 2015 to 2018**  
- **Method:** Grouped crimes by **year and month**.  
- **Visualization:** **Multi-year bar chart comparing monthly trends**.  
- **Insights:**  
  - **2018 shows fewer reported crimes**, possibly due to incomplete data.  
  - **2015 and 2017 had the highest crime rates**.  
  - **Late-year months (Nov–Dec) are unexpectedly high in crime**.  

---

### **5️. Crime risk by time of day: When is SF safest?**  
- **Method:** Filtered incidents on **December 15** across multiple years.  
- **Visualization:** **Hourly line graph comparing crime trends per year**.  
- **Insights:**  
  - **Early mornings are the safest time to be out**.  
  - **Noon and rush hours show peak crime rates**.  
  - **Late-night hours are high-risk; avoid being outside.**  

---

### **6️. Which districts are the most dangerous, and what crimes dominate them?**  
- **Method:**  
  - Identified **top-3 high-crime districts**.  
  - Filtered **most frequent crime types by hour** within these districts.  
- **Visualization:** **Line plot showing hourly crime patterns by type**.  
- **Insights:**  
  - **Southern, Mission, and Northern districts have the highest crime rates.**  
  - **Crime surges between 11 AM – 1 PM & 4 PM – 9 PM**, suggesting ideal times for increased police presence.  

---

### **7️. Crime resolution rates: What gets solved?**  
- **Method:**  
  - Analyzed **resolution status** for each crime category.  
  - Calculated **percentage of resolved cases** per category.  
- **Visualization:** **Bar plot ranking crime categories by resolution rate**.  
- **Insights:**  
  - **DUIs and in-progress offenses** have the highest resolution rates.  
  - **Theft and non-violent offenses remain largely unresolved.**  
  - **Surveillance investment** could improve **theft and robbery resolution rates**.  

---

### **8️. Exploring lesser-known features (e.g., Fire Prevention Districts, Police Jurisdictions)**  
- **Method:** Identified **patterns in additional location-based variables**.  
- **Insights:**  
  - Crime trends vary across **police districts & fire prevention areas**.  
  - This data could be leveraged to **improve district-level crime response planning**.  

---

## Key Takeaways & Policy Suggestions  
✔ **Crime is highest in Southern SF; Richmond is relatively safe.**  
✔ **Theft is the dominant crime, but least likely to be resolved.**  
✔ **Crime peaks at noon, rush hours, and late at night.**  
✔ **Policing efforts should focus on crime-heavy time slots (11 AM–1 PM & 4 PM–9 PM).**  
✔ **Increased surveillance may improve theft and robbery resolution rates.**  

---
