COL_DTYPE = dict(
    caseCode=dict(dtype="String", default=""),
    age=dict(dtype="Integer", default=""),
    ageGroup=dict(dtype="String", default=""),
    sex=dict(dtype="Enum"),
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

COL_MAP = dict(Admitted="isAdmitted", Quarantined="isQuarantined", Pregnanttab="isPregnant")

SEX_ENUM = ["male", "female"]
REMOVAL_TYPE_ENUM = ["recovered", "died"]
HEALTH_STATS_ENUM = ["recovered", "died", "confirmed"]
