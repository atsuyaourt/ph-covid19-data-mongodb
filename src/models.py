SEX_ENUM = ["male", "female"]
REMOVAL_TYPE_ENUM = ["recovered", "died"]
HEALTH_STATS_ENUM = ["recovered", "died", "confirmed"]

CASE_SCHEMA = dict(
    caseCode=dict(dtype="String", default=""),
    age=dict(dtype="Integer", default=""),
    ageGroup=dict(dtype="String", default=""),
    sex=dict(dtype="Enum", default=""),
    dateSpecimen=dict(dtype="Date", default=0),
    dateResultRelease=dict(dtype="Date", default=0),
    dateRepConf=dict(dtype="Date", default=0),
    dateDied=dict(dtype="Date", default=0),
    dateRecover=dict(dtype="Date", default=0),
    removalType=dict(dtype="Enum", default="unknown"),
    dateRepRem=dict(dtype="Date", default=0),
    isAdmitted=dict(dtype="Bool"),
    regionRes=dict(dtype="String", default=""),
    provRes=dict(dtype="String", default=""),
    cityMunRes=dict(dtype="String", default=""),
    cityMuniPSGC=dict(dtype="String", default=""),
    barangayRes=dict(dtype="String", default=""),
    barangayPSGC=dict(dtype="String", default=""),
    healthStatus=dict(dtype="Enum", default="unknown"),
    isQuarantined=dict(dtype="Bool"),
    dateOnset=dict(dtype="Date", default=0),
    isPregnant=dict(dtype="Bool"),
    validationStatus=dict(dtype="String", default=""),
)

CASE_FIELD_MAP = dict(Admitted="isAdmitted", Quarantined="isQuarantined", Pregnanttab="isPregnant")

REGION_MAP = {
    'Region I: Ilocos Region': 'Ilocos Region (Region I)',
    'Region II: Cagayan Valley': 'Cagayan Valley (Region II)',
    'Region III: Central Luzon': 'Central Luzon (Region III)',
    'Region IV-A: CALABARZON': 'CALABARZON (Region IV-A)',
    'Region IV-B: MIMAROPA': 'MIMAROPA (Region IV-B)',
    'Region V: Bicol Region': 'Bicol Region (Region V)',
    'Region VI: Western Visayas': 'Western Visayas (Region VI)',
    'Region VII: Central Visayas': 'Central Visayas (Region VII)',
    'Region VIII: Eastern Visayas': 'Eastern Visayas (Region VIII)',
    'Region IX: Zamboanga Peninsula': 'Zamboanga Peninsula (Region IX)',
    'Region X: Northern Mindanao': 'Northern Mindanao (Region X)',
    'Region XI: Davao Region': 'Davao Region (Region XI)',
    'Region XII: SOCCSKSARGEN': 'SOCCSKSARGEN (Region XII)',
    'CARAGA': 'Caraga (Region XIII)',
    'CAR': 'Cordillera Administrative Region (CAR)',
    'NCR': 'Metropolitan Manila',
    '4B': 'MIMAROPA (Region IV-B)',
    'MIMAROPA': 'MIMAROPA (Region IV-B)',
    'BARMM': 'Autonomous Region of Muslim Mindanao (ARMM)',
    'REPATRIATE': 'Repatriate',
    'Repatriate': 'Repatriate',
    'ROF': 'ROF',
    '': ''
}

REGION_UNKNOWN = ['ROF', 'Repatriate', '']
