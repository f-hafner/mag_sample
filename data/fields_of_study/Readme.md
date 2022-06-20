Corresponding Fields of Study
===============================


This document describes how to correspond the fields of study from the OECD (and used by Web of Science and ProQuest) to fields of study, level 0, in Microsoft Academic Graph (MAG). It has been done by hand.
The goal is to map fields of study in ProQuest Dissertations&Theses (henceforth ProQuest) to the fields of study level 0 in MAG.

## Procedure
1. Download oecd_classification.pdf from [this link](https://www.oecd.org/science/inno/38235147.pdf) on June 20, 2022.
2. Hand-edit crosswalk_wos_mag.xls. It has two tabs: wos_categories and wos_mag.
    - wos_categories: based on oecd_classification.pdf, maps each keyword to their major field. For instance, "pure mathematics" to "mathematics". 
    - wos_mag: correspondence from major field in Web of Science (wos_id) to field_mag (level 0). This is done by hand.  See below for explanations.
3. Export the two tabs to the csv files with the same name.
4. Currently, copies of these files live in `/mnt/ssd/Misc/` which we use for data processing. See the file `./src/dataprep/main/load_proquest/correspond_fieldsofstudy`.py for how we use them.


### The meaning of link flags
The column "link" in the tab "wos_mag" flags how the link was created:
1. "direct": an obvious direct link
2. "not_in_proquest": left out because does not occur in ProQuest.
3. "mag crosswalk" -- see below
4. "mag_lvl" -- see below
5. guessed: no clear correspondence, guessed instead. 


**"mag crosswalk"**
This means that we used our custom correspondence between levels of fields of study in MAG to determine the link. For instance, the link "animal science -> biology" can be found in the database as follows:

```sql
select parentfieldofstudyid, childfieldofstudyid, parent_name, child_name
from crosswalk_fields a
inner join (
    select fieldofstudyid, normalizedname as parent_name
    from fieldsofstudy) b
on (a.parentfieldofstudyid = b.fieldofstudyid)
inner join (
    select fieldofstudyid, normalizedname as child_name 
    from fieldsofstudy) c
on (a.childfieldofstudyid = c.fieldofstudyid)
where parent_name = "biology" and child_name = "animal science"
```

You can find details on how we created the table "crosswalk_fields" in the script `./src/dataprep/main/prep_mag/paper_fields.py`.

**"mag_lvl1"**
This means that the wos_fields is at the highest level in MAG (sociology). A better name would have been "mag_lvl0" because MAG levels are 0-indexed, but it's too late now. 

