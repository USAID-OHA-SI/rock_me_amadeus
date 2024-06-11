USE mozart_fgh_zam_test;

SHOW tables;

DESCRIBE type_id_lookup;

select distinct table_name, column_name
from type_id_lookup
order by table_name;

select distinct table_name, column_name
from type_id_lookup
where table_name = 'MEDICATION';

/*
table_name, column_name
MEDICATION, REGIMEN_ID
MEDICATION, FORMULATION_ID
MEDICATION, MODE_DISPENSATION_ID
MEDICATION, MED_LINE_ID
MEDICATION, TYPE_DISPENSATION_ID
MEDICATION, ALTERNATIVE_LINE_ID
MEDICATION, REASON_CHANGE_REGIMEN_ID
MEDICATION, ARV_SIDE_EFFECT_ID
MEDICATION, ADHERENCE_ID
*/

select distinct id_type_lookup as regimen_id, id_type_desc as regimen_code
from type_id_lookup
where table_name = 'MEDICATION' and column_name = 'REGIMEN_ID'
order by regimen_code;

/* MEDICATIONS */

describe medication;

/*
id	int	NO	PRI		auto_increment
encounter_uuid	char(38)	YES	MUL		
regimen_id	int	YES			
formulation_id	int	YES			
quantity_prescribed	double	YES			
dosage	text	YES			
medication_pickup_date	datetime	YES			
next_pickup_date	datetime	YES			
mode_dispensation_id	int	YES			
med_sequence_id	int	YES			
type_dispensation_id	int	YES			
alternative_line_id	int	YES			
reason_change_regimen_id	int	YES			
med_side_effects_id	int	YES			
adherence_id	int	YES			
medication_uuid	char(38)	YES	UNI		
*/

select * from medication limit 10;

select 
	distinct
	year(medication_pickup_date) as pickup_year,
    month(medication_pickup_date) as pickup_month,
    day(medication_pickup_date) as pickup_day
from medication
order by pickup_year desc, pickup_month asc, pickup_day asc;

select distinct regimen_id, formulation_id from medication;

select distinct regimen_id, formulation_id, mode_dispensation_id 
from medication
where regimen_id is not null
order by regimen_id;

/* Add ref values to medication table */

create or replace view medication_vw as
select 
	m.id, m.encounter_uuid, m.medication_uuid, m.quantity_prescribed as quantity, 
	m.dosage, m.medication_pickup_date as pickup_date, m.next_pickup_date,
	m.regimen_id, ireg.id_type_desc as regimen_code,
	m.formulation_id, iform.id_type_desc as formulation_name,
	m.mode_dispensation_id, imod.id_type_desc as mode_dispensation_name,
	m.type_dispensation_id, iseq.id_type_desc as type_dispensation_name,
	m.alternative_line_id, iseq.id_type_desc as alternative_line_name,
	m.reason_change_regimen_id, iseq.id_type_desc as reason_change_regimen,
	m.med_side_effects_id, iseq.id_type_desc as med_side_effects,
	m.adherence_id, iseq.id_type_desc as adherence_name
from medication m 
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'MEDICATION' and column_name = 'REGIMEN_ID') ireg
on m.regimen_id = ireg.id_type_lookup 
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'MEDICATION' and column_name = 'FORMULATION_ID') iform
on m.formulation_id = iform.id_type_lookup 
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'MEDICATION' and column_name = 'MODE_DISPENSATION_ID') imod
on m.mode_dispensation_id = imod.id_type_lookup 
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'MEDICATION' and column_name = 'MODE_SEQUENCE_ID') iseq
on m.med_sequence_id = iseq.id_type_lookup 
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'MEDICATION' and column_name = 'TYPE_DISPENSATION_ID') itdisp
on m.type_dispensation_id = itdisp.id_type_lookup 
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'MEDICATION' and column_name = 'ALTERNATIVE_LINE_ID') ialt
on m.alternative_line_id = ialt.id_type_lookup
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'MEDICATION' and column_name = 'REASON_CHANGE_REGIMEN_ID') icreg
on m.reason_change_regimen_id = icreg.id_type_lookup
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'MEDICATION' and column_name = 'MED_SIDE_EFFECTS_ID') imeff
on m.med_side_effects_id = imeff.id_type_lookup
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'MEDICATION' and column_name = 'ADHERENCE_ID') iadh
on m.adherence_id = iadh.id_type_lookup
order by pickup_date desc;

select * from medication_vw 
where 
	regimen_id is not null and 
    quantity is not null and 
    dosage is not null and 
    pickup_date is not null and 
    year(pickup_date) = 2024
limit 100;

/* MEDICATION: replicate sample query from session #2 */

/*
WITH med_dispensations as (
	SELECT 
    m.location_uuid,
    m.encounter_uuid,
    m.patient_uuid,
    p.birthdate,
    p.gender,
    m.form_id,
    m.regimen_id,
    m.mode_dispensation_id,
    m.medication_pickup_date,
    m.next_pickup_date,
    row_number() over (PARTITION BY p.patient_uuid ORDER BY m.next_pickup_date DESC, m.medication_pickup_date DESC) as row_num
    FROM medication_wfd PARTITION(medication_wfd_y2023) m
    LEFT JOIN patient p ON m.patient_uuid = p.patient_uuid
    WHERE (m.next_pickup_date > '2023-12-20 00:00:00' and m.next_pickup_date < '2024-12-20 00:00:00' AND m.form_id = 130)
)
select * from med_dispensations
where row_num = 1;
*/


SELECT 
	p.patient_uuid,
    f.location_uuid,
    m.encounter_uuid,
    f.form_id,
    iform.id_type_desc as form_name,
	f.encounter_type,
    ienc.id_type_desc as encounter_name,
    p.birthdate,
    p.gender,
    m.regimen_id,
    m.mode_dispensation_id,
    m.medication_pickup_date,
    m.next_pickup_date
FROM medication m
LEFT JOIN form f ON m.encounter_uuid = f.encounter_uuid
LEFT JOIN patient p ON f.patient_uuid = p.patient_uuid
LEFT JOIN (SELECT id_type_lookup, id_type_desc FROM type_id_lookup WHERE table_name = 'FORM' AND column_name = 'FORM_ID') iform
ON f.form_id = iform.id_type_lookup
LEFT JOIN (SELECT id_type_lookup, id_type_desc FROM type_id_lookup WHERE table_name = 'FORM' AND column_name = 'ENCOUNTER_TYPE') ienc
ON f.encounter_type = ienc.id_type_lookup
WHERE year(m.medication_pickup_date) = 2024 AND f.form_id = 130;

--- FORM --- 

describe form;

--- drop view if exists form_vw;

create or replace view form_vw as
select 
f.id, f.encounter_id, f.encounter_uuid, f.patient_uuid,
f.form_id, ifid.id_type_desc as form_name,
f.encounter_type, ietype.id_type_desc as encounter_type_name,
f.encounter_date, f.change_date, f.location_uuid as form_locaton_uuid, f.source_database
from form f
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'FORM' and column_name = 'FORM_ID') ifid
on f.form_id = ifid.id_type_lookup
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'FORM' and column_name = 'ENCOUNTER_TYPE') ietype
on f.encounter_type = ietype.id_type_lookup;

select * from form_vw;

/** 
Different type of forms: 
	130 (FILA) & 166 (Levantou) used for medication dispensations 
    163 (FICHE CLINICA) used for clinical consultations
**/

select distinct 
form_id, form_name, 
encounter_type, encounter_type_name 
from form_vw
order by form_id asc;


--- CLINICAL CONSULTATION ---

describe clinical_consultation;

/*
id	int	NO	PRI		auto_increment
encounter_uuid	char(38)	NO	UNI		
consultation_date	date	NO			
scheduled_date	date	YES			
bp_diastolic	double	YES			
bp_systolic	double	YES			
who_staging	int	YES			
weight	double	YES			
height	double	YES			
arm_circumference	double	YES			
nutritional_grade	int	YES			
*/

select * from clinical_consultation limit 100;

select distinct bp_diastolic, bp_systolic from clinical_consultation;

/* FORM: Where are the look up values */

select distinct table_name, column_name
from type_id_lookup
where table_name = 'FORM';

/* Clinical Consultation: Join form info & Add / Replace form */

--- Option #1: Join reference tables individually --

/* drop view if exists clinical_consultation_vw; */

create or replace view clinical_consultation_vw as
select 
cc.id, cc.encounter_uuid, f.encounter_date, cc.consultation_date, cc.scheduled_date,
f.form_id, ifid.id_type_desc as form_name,
f.encounter_type, ietype.id_type_desc as encounter_type_name
from clinical_consultation cc
left join form f 
on cc.encounter_uuid = f.encounter_uuid
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'FORM' and column_name = 'FORM_ID') ifid
on f.form_id = ifid.id_type_lookup
left join (select id_type_lookup, id_type_desc from type_id_lookup where table_name = 'FORM' and column_name = 'ENCOUNTER_TYPE') ietype
on f.encounter_type = ietype.id_type_lookup;

select * from clinical_consultation_vw;

--- Option #2: Leverage the form view --- 

select 
cc.id, cc.encounter_uuid, f.encounter_date, cc.consultation_date, cc.scheduled_date, f.*
from clinical_consultation cc
left join form_vw f 
on cc.encounter_uuid = f.encounter_uuid;

select 
distinct encounter_type, encounter_type_name 
from form_vw
order by encounter_type;

select 
distinct 
encounter_type, encounter_type_name,
form_id, form_name
from form_vw
order by encounter_type, form_id;


