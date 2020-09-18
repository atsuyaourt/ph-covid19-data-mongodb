# DOH COVID-19 Case Information Data for the Philippines

Contains scripts for processing COVID-19 data from the [DOH](https://www.doh.gov.ph/) [data drop](http://bit.ly/DataDropPH).
  
**Start Date:** `June 9, 2020`  
**End Date:** `N/A`

## MongoDB
- URI = mongodb+srv://\<username\>:\<password\>@ph-covid-19-o6bwa.mongodb.net/default?retryWrites=true&w=majority  
- username = viewer01, viewer02  
- password = 1WearMask

You can also contact me for the latest mongodump


## Fields:
| Field Name        | Description                                                              | Type    |
| ----------------- | ------------------------------------------------------------------------ | ------- |
| caseCode          | Random code assigned for labelling cases                                 | String  |
| age               | Age                                                                      | Integer |
| sex               | Sex                                                                      | Enum    |
| dateSpecimen      | Date when specimen was collected                                         | Date    |
| dateResultRelease | Date when result is released                                             | Date    |
| dateRepConf       | Date publicly announced as confirmed case                                | Date    |
| dateDied          | Date died                                                                | Date    |
| dateRecover       | Date recovered                                                           | Date    |
| removalType       | Type of removal:                                                         | Enum    |
|                   | [recovered, died]                                                        |         |
| isAdmitted        | Yes if patient has been admitted to hospital                             | Boolean |
| regionRes         | Region of residence                                                      | String  |
| provRes           | Province of residence                                                    | String  |
| cityMunRes        | City of residence                                                        | String  |
| cityMuniPSGC      | Philippine Standard Geographic Code of Municipality or City of Residence | String  |
| barangayRes       | Barangay of residence                                                    | String  |
| barangayPSGC      | Philippine Standard Geographic Code of Barangay of Residence             | String  |
| healthStatus      | Known current health status of patient                                   | Enum    |
|                   | [asymptomatic, mild, severe, critical, died, recovered]                  |         |
| isQuarantined     | Yes if home quarantined                                                  | Boolean |
| dateOnset         | Date of onset of symptoms                                                | Date    |
| isPregnant        | Yes if patient is pregnant at any point during COVID-19 condition        | Boolean |
| ----------------- | **Additional Fields**                                                    | ------- |
| ----------------- | ------------------------------------------------------------------------ | ------- |
| createdAt         | Date added to database                                                   | Date    |
| updatedAt         | Date updated in the database                                             | Date    |
| deletedAt         | Date marked as invalid in the database                                   | Date    |
| regionResGeo      | Region of residence                                                      | Loc     |
| provResGeo        | Province of residence                                                    | Loc     |
| cityMunResGeo     | City of residence                                                        | Loc     |


- Indices: `createdAt`, `caseCode`, `healthStatus`
- No Date: `ISODate("1970-01-01T00:00:00 PHT")`
- Entries identified by DOH as duplicates have the following field values:
  
    ```js
    {  
        deletedAt: { $exists: 1 },
        healthStatus: "invalid",  
        removalType: "duplicate"  
    }
    ```