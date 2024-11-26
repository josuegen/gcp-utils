/******************************************************************************************************************************
Builds a Success Factors Employee Central master table,
using nested and repeated fields. Used the documentation below to build the structure

https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/e1209c55c4034e3cb6178dcea3faa361.html

Author: Josue Velazquez (josuegen@google.com)
********************************************************************************************************************************/


CREATE OR REPLACE TABLE <project>.<dataset>.employee_central
CLUSTER BY company,businessUnit,emplStatus,userId
AS 
SELECT
  empjob.*,
  empstatus.label_es_MX AS emplStatus,
  contract_type.label_es_MX AS contractType,
  pickeve.label_es_MX AS event,
  countrylistcompany.label_es_MX AS countryOfCompany_label,
  STRUCT(
    ee.assignmentClass,
    ee.assignmentIdExternal,
    ee.benefitsEligibilityStartDate,
    ee.customDate1 AS Vacation_Bonus_Date,
    ee.customDate2 AS Seniority_Date,
    ee.customDate21 AS Cambio_de_regimen,
    ee.customDate3 AS Fecha_Certificado_Vac,
    ee.customDate4 AS Antiguedad_para_recibo_de_haberes,
    ee.customDate5 AS Termino_del_periodo_adquisitivo,
    ee.customDate6 AS Baja_de_la_Caja,
    ee.customDate7 AS Baja_de_Vacaciones,
    ee.customLong1 AS Dias_de_vacaciones_por_Contrato,
    ee.customString16 AS Event_Reason_Detailed,
    ee.customString2 AS TAM_Flag,
    ee.employmentId,
    ee.endDate,
    ee.firstDateWorked,
    ee.hiringNotCompleted,
    ee.includeAllRecords,
    ee.isContingentWorker,
    ee.isECRecord,
    ee.jobNumber,
    ee.lastDateWorked,
    ee.okToRehire,
    ee.originalStartDate,
    ee.payrollEndDate,
    ee.personIdExternal,
    ee.seniorityDate,
    ee.serviceDate,
    ee.startDate,
    ee.userId
  ) AS EmpEmployment,
  STRUCT(
    person.countryOfBirth,
    person.customString4 AS Site_Of_Birth,
    person.dateOfBirth,
    person.perPersonUuid,
    person.personId,
    person.personIdExternal,
    person.placeOfBirth,
    person.regionOfBirth
  ) AS PerPerson,
  STRUCT(
    personal.startDate,
    personal.displayName,
    personal.endDate,
    personal.firstName,
    personal.formalName,
    personal.gender AS gender_code,
    personal.gender_label,
    personal.lastName,
    personal.maritalStatus AS marital_status_code,
    personal.marital_status_label,
    personal.middleName,
    personal.nationality,
    personal.nativePreferredLang,
    personal.salutation,
    personal.secondLastName,
    personal.secondNationality,
    personal.thirdNationality
  ) AS PerPersonal,
  STRUCT(
    peraddress.address1,
    peraddress.address10,
    peraddress.address11,
    peraddress.address12,
    peraddress.address2,
    peraddress.address3,
    peraddress.address4,
    peraddress.address5,
    peraddress.address6,
    peraddress.address7,
    peraddress.address8,
    peraddress.address9,
    peraddress.addressType,
    peraddress.attachmentId,
    peraddress.city,
    peraddress.country,
    peraddress.county,
    peraddress.customString1 AS Type_Abode,
    peraddress.customString2 AS Type_of_Abode,
    peraddress.customString3 AS Type_Abode_1,
    peraddress.endDate,
    peraddress.notes,
    peraddress.province,
    peraddress.startDate,
    peraddress.state,
    peraddress.state_label,
    peraddress.zipCode
  ) AS PerAddressDEFLT,
  STRUCT(
    email.emailAddress,
    email.emailType,
    email.isPrimary
  ) AS PerEmail,
  STRUCT(
    phone.countryCode,
    phone.isPrimary,
    phone.phoneNumber,
    phone.phoneType
  ) AS PerPhone,
  STRUCT(
    national_id.cardType,
    national_id.country,
    national_id.isPrimary,
    national_id.nationalId,
    national_id.notes
  ) PerNationalId,
  STRUCT(
    flocgr.externalCode,
    flocgr.TimeTypeProfileFlx,
    flocgr.companyFlx,
    flocgr.custRegionFlx,
    flocgr.custWorkCenterFlx,
    flocgr.customString1 AS Address,
    flocgr.customString2 AS ZIP_Code,
    flocgr.customString3 AS City,
    flocgr.customString4 AS Country_Code,
    flocgr.country_label AS Country_Label,
    flocgr.customString5 AS State_Code,
    flocgr.state_label AS State_Label,
    flocgr.customString6 AS Time_Zone,
    flocgr.description,
    flocgr.endDate,
    flocgr.geozoneFlx,
    flocgr.internalCode,
    flocgr.name,
    flocgr.objectId,
    flocgr.status AS status_code,
    flocgr.employee_status_label AS status_label
  ) AS FOLocationGroup,
  STRUCT(
    fcomp.externalCode,
    fcomp.country,
    fcomp.currency,
    fcomp.endDate,
    fcomp.entityOID,
    fcomp.mdfSystemRecordId,
    fcomp.name,
    fcomp.name_defaultValue,
    fcomp.name_en_DEBUG,
    fcomp.name_en_US,
    fcomp.name_es_MX,
    fcomp.name_localized,
    fcomp.name_pt_BR,
    fcomp.officialLanguage,
    fcomp.status,
    fcomp.toDisplayNameFormatProp,
    fcomp.toNameFormatProp
  ) AS FOCompany,
  STRUCT(
    fbusun.externalCode,
    fbusun.startDate,
    fbusun.description,
    fbusun.description_defaultValue,
    fbusun.description_en_US,
    fbusun.description_es_MX,
    fbusun.description_localized,
    fbusun.description_pt_BR,
    fbusun.endDate,
    fbusun.entityUUID,
    fbusun.name,
    fbusun.name_defaultValue,
    fbusun.name_en_US,
    fbusun.name_es_MX,
    fbusun.name_localized,
    fbusun.name_pt_BR,
    fbusun.status
  ) AS FOBusinessUnit,
  STRUCT(
    emcla.externalCode,
    emcla.effectiveStartDate,
    emcla.externalName_defaultValue,
    emcla.externalName_en_DEBUG,
    emcla.externalName_en_US,
    emcla.externalName_es_MX,
    emcla.externalName_localized,
    emcla.externalName_pt_BR
  ) AS cust_EmployeeClass,
  STRUCT(
    emty.externalCode,
    emty.effectiveStartDate,
    emty.externalName_defaultValue,
    emty.externalName_en_DEBUG,
    emty.externalName_en_US,
    emty.externalName_es_MX,
    emty.externalName_localized,
    emty.externalName_pt_BR
  ) AS cust_EmployeeType,
  STRUCT(
    position.businessUnit,
    position.changeReason,
    position.code,
    position.comment,
    position.company,
    position.costCenter,
    position.criticality,
    position.cust_AsignacionCliente,
    position.cust_BusinessArea,
    position.cust_BusinessAreaAux,
    position.cust_CBO,
    position.cust_COL_Depto,
    position.cust_COL_Mpio,
    position.cust_COL_Tipo_Oper,
    position.cust_CrewID,
    position.cust_Family,
    position.cust_IDCell,
    position.cust_IDProcessHom,
    position.cust_IDProcessType,
    position.cust_LugarFisico,
    position.cust_LugarFisicoAP,
    position.cust_PersonalType,
    position.cust_PositionHO,
    position.cust_SEDE,
    position.cust_ServicioFEMSA,
    position.cust_SubZone,
    position.cust_Subfamily,
    position.cust_TipoDeOperacion,
    position.cust_TipoNominaCSCP,
    position.cust_Tipo_de_Correo,
    position.cust_Zona_AP,
    position.cust_businessUnitFP,
    position.cust_contributionLevel,
    position.contributionLevel_label,
    position.cust_costCenterAux,
    position.cust_duration,
    position.cust_employeeClass,
    position.cust_employmentType,
    position.cust_familiaFP,
    position.cust_fechaFin,
    position.cust_functionalArea,
    position.cust_functionalSubarea,
    position.cust_groupIDTitleHom,
    position.cust_idHomologousPosition,
    position.cust_incentiveCode,
    position.cust_incentivePlan,
    position.cust_isFullTimeEmp,
    position.cust_locationGroup,
    position.cust_max,
    position.cust_mid,
    position.cust_min,
    position.cust_nivelContribucionFP,
    position.cust_nivelOrganizacionalFP,
    position.cust_nombreHomologousPuesto,
    position.cust_payScaleArea,
    position.cust_payScaleGroup,
    position.cust_payScaleLevel,
    position.cust_payScaleType,
    position.cust_positionType,
    position.cust_region,
    position.cust_shortName,
    position.cust_subfamiliaFP,
    position.cust_zone,
    position.department,
    position.description,
    position.division,
    position.effectiveEndDate,
    position.effectiveStartDate,
    position.effectiveStatus,
    position.employeeClass,
    position.externalName_defaultValue,
    position.externalName_en_US,
    position.externalName_es_MX,
    position.externalName_localized,
    position.externalName_pt_BR,
    position.incumbent,
    position.jobCode,
    position.jobLevel,
    position.jobTitle,
    position.legacyPositionId,
    position.location,
    position.multipleIncumbentsAllowed,
    position.payGrade,
    position.payRange,
    position.positionControlled,
    position.positionCriticality,
    position.positionTitle,
    position.regularTemporary,
    position.standardHours,
    position.targetFTE,
    position.technicalParameters,
    position.transactionSequence,
    position.type,
    position.vacant
  ) AS Position,
  STRUCT(
    epcr.currencyCode,
    epcr.effectiveLatestChange,
    epcr.endDate,
    epcr.frequency AS frequency_code,
    epcr.frequency_label,
    epcr.notes,
    epcr.payComponent,
    epcr.paycompvalue,
    epcr.seqNumber,
    epcr.startDate
  ) AS EmpPayCompRecurring,
  STRUCT(
    eventreason.description,
    eventreason.emplStatus,
    eventreason.endDate,
    eventreason.event,
    eventreason.externalCode,
    eventreason.implicitPositionAction,
    eventreason.includeInWorkExperience,
    eventreason.internalCode,
    eventreason.name,
    eventreason.objectId,
    eventreason.payrollEvent,
    eventreason.startDate,
    eventreason.status
  ) AS FOEventReason,
  STRUCT(
    dept.costCenter,
    dept.cust_AuxCostCenter,
    dept.cust_BusinessArea,
    dept.cust_BusinessAreaAux,
    dept.cust_District,
    dept.cust_jobFunction,
    dept.cust_legalEntity,
    dept.cust_location,
    dept.cust_locationGroup,
    dept.cust_shortName,
    dept.description,
    dept.description_defaultValue,
    dept.description_en_US,
    dept.description_es_MX,
    dept.description_localized,
    dept.description_pt_BR,
    dept.endDate,
    dept.entityUUID,
    dept.externalCode,
    dept.headOfUnit,
    dept.name,
    dept.name_defaultValue,
    dept.name_en_US,
    dept.name_es_MX,
    dept.name_localized,
    dept.name_pt_BR,
    dept.parent,
    dept.startDate,
    dept.status
  ) AS FODepartment
FROM (
  SELECT
    assedicCertInitialStateNum,
    assedicCertObjectNum,
    businessUnit,
    calcMethodIndicator,
    commitmentIndicator,
    company,
    continuedSicknessPayMeasure,
    continuedSicknessPayPeriod,
    contractEndDate,
    contractReferenceForAed,
    contractType AS contractType_code,
    costCenter,
    countryOfCompany AS countryOfCompany_code,
    customDate1 AS Position_Date_For_Talent,
    customDate9 AS Data_Aposentaduria,
    customLong1 AS Time_Management_Status,
    customLong2 AS Travel_Privileges,
    customLong3 AS Working_Days_Per_Week,
    customString10 AS Time_manager,
    customString101 AS Edificio_ALPUNTO,
    customString12 AS Location_Group,
    customString13 AS Contribution_Level,
    customString139 AS Tipo,
    customString14 AS Functional_Area,
    customString145 AS Departamentos,
    customString146 AS Municipios,
    customString147 AS Tipo_de_operacion,
    customString148 AS ActiveNumber,
    customString149 AS Internship_Type,
    customString15 AS Subfunctional_Area,
    customString151 AS Modalidad_de_Trabajo,
    customString152 AS Tipo_de_Operacion_1,
    customString153 AS Zona_Estado_ALPUNTO,
    customString154 AS Localidad_o_Municipio,
    customString155 AS AF_Ceco,
    customString156 AS PEM,
    customString157 AS Unemployment_insurance,
    customString158 AS RelLaboral,
    customString159 AS Physical_place,
    customString16 AS Position_Type,
    customString160 AS Termo_Vale_Refeicao_Eletronico,
    customString17 AS Business_Area,
    customString18 AS Employee_Class_Group_for_WS,
    customString19 AS Location_Group_Group_for_WS,
    customString2 AS Release_Code,
    customString20 AS Work_Schedule_Rule_Grouping_for_WS,
    customString21 AS Employee_Class,
    customString22 AS Employee_Type,
    customString23 AS District,
    customString24 AS Work_Center,
    customString25 AS Zone,
    customString26 AS Auxiliary_Cost_Center,
    customString27 AS `Group`,
    customString28 AS Region,
    customString29 AS Benefit_Group_1,
    customString3 AS Personnel_Type,
    customString30 AS Benefit_Group_2,
    customString31 AS Positon_ID,
    customString32 AS HR_Manager,
    customString33 AS Manager,
    customString34 AS Custom_Manager_1,
    customString35 AS Custom_Manager_2,
    customString36 AS Matrix_Manager_1,
    customString37 AS Matrix_Manager_2,
    customString38 AS UserID_1,
    customString39 AS Nomina_Site,
    customString40 AS CeCo_Tienda_de_Entrenamiento,
    customString41 AS Division_LMS,
    customString42 AS Encargado_de_Personal,
    customString43 AS ID_Cell,
    customString44 AS Crew_ID,
    customString45 AS Language_Assigned_at_Hire,
    customString46 AS Organizational_Level,
    customString47 AS Contribution_Level_1,
    customString48 AS Family,
    customString49 AS Subfamily,
    customString66 AS Tipo_de_Correo,
    customString7 AS SubZones,
    customString8 AS Additional_Time_Indicator,
    department,
    division,
    educationalEntity,
    eeo1JobCategory,
    eeo4JobCategory,
    eeo5JobCategory,
    eeo6JobCategory,
    eeoClass,
    effectiveLatestChange,
    electoralCollegeForLaborCourt,
    electoralCollegeForWorkersRepresentatives,
    electoralCollegeForWorksCouncil,
    empRelationship,
    emplStatus AS emplStatus_code,
    endDate,
    event AS event_code,
    eventReason,
    exclExecutiveSector,
    familyRelationshipWithEmployer,
    fgtsDate,
    fgtsOptant,
    fgtsPercent,
    flsaStatus,
    fte,
    harmfulAgentExposure,
    hazard,
    healthRisk,
    holidayCalendarCode,
    integrationAgent,
    internshipLevel,
    internshipSchool,
    isCompetitionClauseActive,
    isPrimary,
    jobCode,
    jobTitle,
    laborCourtSector,
    location,
    managerCategory,
    managerId,
    mandatoryInternship,
    notes,
    noticePeriod,
    noticePeriodStartDate,
    operation,
    payGrade,
    payGroup,
    payScaleArea,
    payScaleGroup,
    payScaleLevel,
    payScaleLevelEntryDate,
    payScaleType,
    pcfm,
    pensionProtection,
    periodIndicator,
    position AS position_code,
    positionEntryDate,
    probationPeriodEndDate,
    retired,
    seqNumber,
    sickPaySupplement,
    standardHours,
    startDate,
    teachersPension,
    timeRecordingProfileCode,
    timeRecordingVariant,
    timeTypeProfileCode,
    timezone,
    travelDistance,
    tupeOrgNumber,
    userId,
    workPermitExpiry,
    workerCategory,
    workingTimeDirective,
    workscheduleCode,
    wtdHoursLimit
  FROM `<project>.<dataset>.EmpJob`
) AS empjob
LEFT JOIN
  `<project>.<dataset>.EmpEmployment` AS ee
ON
  empjob.userId= ee.userId
LEFT JOIN
  `<project>.<dataset>.employee_status` AS emp_status
ON
  empjob.emplStatus_code = CAST(emp_status.optionId AS STRING)
LEFT JOIN
  `<project>.<dataset>.contractType` AS contract_type
ON
  empjob.contractType_code = CAST(contract_type.optionId AS STRING)
LEFT JOIN
  `<project>.<dataset>.event` AS pickeve
ON
  empjob.event_code = CAST(pickeve.optionId AS STRING)
LEFT JOIN
  `<project>.<dataset>.PerPerson` AS person
ON
  ee.personIdExternal= person.personIdExternal
LEFT JOIN
  (
    SELECT
      per.*,
      gender.label_es_MX AS Gender_label,
      maritalstatus.label_es_MX AS Marital_Status_label
    FROM `<project>.<dataset>.PerPersonal` per
    LEFT JOIN `<project>.<dataset>.PickList_gender` gender
    ON per.gender = SUBSTR(gender.externalCode,1,1)
    LEFT JOIN `<project>.<dataset>.PickList_MaritalStatus` maritalstatus
    ON per.maritalStatus = CAST(maritalstatus.optionId AS STRING)
  ) AS personal
ON
  person.personIdExternal= personal.personIdExternal
LEFT JOIN(
  SELECT
    per.*,
    plstate.label_es_MX AS state_label
  FROM `<project>.<dataset>.PerAddressDEFLT` per
  LEFT JOIN `<project>.<dataset>.PickList_State` plstate
  ON per.state = CAST(plstate.optionId AS STRING)
) AS peraddress
ON
  ee.personIdExternal=peraddress.personIdExternal
LEFT JOIN
  `<project>.<dataset>.PerEmail` AS email
ON
  ee.personIdExternal=email.personIdExternal
LEFT JOIN `<project>.<dataset>.PerPhone` phone
ON
  ee.personIdExternal=phone.personIdExternal
LEFT JOIN
  `<project>.<dataset>.PerNationalId` national_id
ON
  ee.personIdExternal=national_id.personIdExternal
LEFT JOIN
  (
    SELECT
      fo.*,
      pl_country.label_es_MX AS country_label,
      pl_state.label_es_MX AS state_label,
      pl_status.label_es_MX AS employee_status_label
    FROM `<project>.<dataset>.FOLocationGroup` fo
    LEFT JOIN `<project>.<dataset>.PickList_ISOCountryList` pl_country
    ON fo.customString4 = CAST(pl_country.optionId AS STRING)
    LEFT JOIN `<project>.<dataset>.PickList_State` pl_state
    ON fo.customString5 = CAST(pl_state.optionId AS STRING)
    LEFT JOIN
    `<project>.<dataset>.PickList_employee_status` pl_status
    ON fo.status = CAST(pl_status.externalCode AS STRING)
  ) AS flocgr
ON
  empjob.Location_Group=flocgr.externalCode
LEFT JOIN
  `<project>.<dataset>.FOCompany` AS fcomp
ON
  empjob.company=fcomp.externalCode
LEFT JOIN
  `<project>.<dataset>.FOBusinessUnit` AS fbusun
ON
  empjob.businessUnit= fbusun.externalCode
LEFT JOIN
  `<project>.<dataset>.FOEventReason` eventreason
ON
  empjob.eventReason=eventreason.externalCode
LEFT JOIN
  `<project>.<dataset>.FODepartment` dept
ON
  empjob.department=dept.externalCode
LEFT JOIN
  `<project>.<dataset>.cust_EmployeeClass` AS emcla
ON
  empjob.Employee_Class=emcla.externalCode
LEFT JOIN
  `<project>.<dataset>.cust_EmployeeType` AS emty
ON
  empjob.Employee_Type=emty.externalCode
LEFT JOIN(
  SELECT
    pos.*,
    contriblevel.label_es_MX AS contributionLevel_label
  FROM `<project>.<dataset>.Position` pos
  LEFT JOIN
  `<project>.<dataset>.PickList_contributionLevel` contriblevel
  ON pos.cust_contributionLevel = contriblevel.externalCode
) AS position
ON
  empjob.position_code=position.code
LEFT JOIN(
  SELECT
    pay.*,
    pl.label_es_MX AS frequency_label
  FROM `<project>.<dataset>.EmpPayCompRecurring` pay
  LEFT JOIN `<project>.<dataset>.PickList_PayFrequency` pl
  ON pay.frequency = pl.externalCode
) AS epcr
ON
  empjob.userId = epcr.userId
LEFT JOIN
  `<project>.<dataset>.PickList_employee_status` empstatus
ON
  empjob.emplStatus_code = CAST(empstatus.optionId AS STRING)
LEFT JOIN
  `<project>.<dataset>.PickList_ISOCountryList` countrylistcompany
ON
  empjob.countryOfCompany_code = countrylistcompany.externalCode
