# DOH COVID-19 Case Information Data for the Philippines

Contains scripts for processing COVID-19 data from the [DOH data drop](http://bit.ly/DataDropPH).

Data updated daily since `June 9, 2020`.

## React App:
* Live: https://emiliogozo.me/ph-covid19-react
* Github: https://github.com/emiliogozo/ph-covid19-react

## Example API:
* Live: https://phcovid19api.carpe-datum.live/v1/docs/
* Github: https://github.com/emiliogozo/ph-covid19-api


## Fields:
| Field Name        | Description                                                              | Type    |
| ----------------- | ------------------------------------------------------------------------ | ------- |
| caseCode          | Random code assigned for labelling cases (PK)                            | String  |
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


## Additional Fields:
| Field Name        | Description                                                              | Type    |
| ----------------- | ------------------------------------------------------------------------ | ------- |
| createdAt         | Date added to database                                                   | Date    |
| updatedAt         | Date updated in     database                                             | Date    |
| locId             | ID of matched mappble location (FK)                                      |         |
