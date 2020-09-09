COL_DTYPE = dict(
    caseCode=dict(dtype="String"),
    age=dict(dtype="Integer"),
    ageGroup=dict(dtype="String"),
    sex=dict(dtype="Enum"),
    dateSpecimen=dict(dtype="Date"),
    dateResultRelease=dict(dtype="Date"),
    dateRepConf=dict(dtype="Date"),
    dateDied=dict(dtype="Date"),
    dateRecover=dict(dtype="Date"),
    removalType=dict(dtype="Enum"),
    dateRepRem=dict(dtype="Date"),
    isAdmitted=dict(dtype="Bool"),
    regionRes=dict(dtype="String"),
    provRes=dict(dtype="String"),
    cityMunRes=dict(dtype="String"),
    cityMuniPSGC=dict(dtype="String"),
    healthStatus=dict(dtype="Enum"),
    isQuarantined=dict(dtype="Bool"),
    dateOnset=dict(dtype="Date"),
    isPregnant=dict(dtype="Bool"),
    validationStatus=dict(dtype="String"),
)

COL_MAP = dict(Admitted="isAdmitted", Quarantined="isQuarantined", Pregnanttab="isPregnant")

SEX_ENUM = ["male", "female"]
REMOVAL_TYPE_ENUM = ["recovered", "died"]
HEALTH_STATS_ENUM = ["recovered", "died", "confirmed"]
