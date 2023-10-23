--get delivered tasks and its details from the main project
with delivered_tasks as (
select 
coalesce(metadata:pvid,concat(metadata:domain, '!', metadata:item_id)) as PVID,
metadata:category as CATEGORY,
--REPLACE(REPLACE(y.value,'_form',''),'form_','') as ATTRIBUTE,
metadata:title as Title,
metadata:link as URL,
_id as DELIVERY_TASK,
REPLACE(x.key,'form_','') as ATTRIBUTE,
case when ATTRIBUTE=REPLACE(y.value:field_id::text,'form_','') THEN 'response' ELSE 'is_cbd' END as RESPONSE_TYPE,
y.value:response[0]::text as TASKER_RESPONSE, 
concat(PVID,ATTRIBUTE,RESPONSE_TYPE) as TASKER_RESPONSE_ID
from SCALE_PROD.PUBLIC.TASKS,
lateral flatten(input => response:annotations) as x,
lateral flatten(input => x.value:response[0]) as y
where project='645dd187d8315806686babc4'),

--get qir tasks and its details from the qir project
qir_tasks as (
select 
coalesce(metadata:pvid,concat(metadata:domain, '!', metadata:item_id)) as PVID,
_ID as QIR_TASK,
REPLACE(a.key,'form_','') as ATTRIBUTE,
case when ATTRIBUTE=REPLACE(b.value:field_id::text,'form_','') THEN 'response' ELSE 'is_cbd' END as RESPONSE_TYPE,
b.value:response[0]::text as QIR_RESPONSE ,
cast(completed_at as DATE) as QIR_RESPONSE_DATE,
concat(PVID,ATTRIBUTE,RESPONSE_TYPE) as QIR_RESPONSE_ID
from SCALE_PROD.PUBLIC.TASKS,
lateral flatten(input => response:annotations) as a,
lateral flatten(input => a.value:response[0]) as b
where project='646276e03005e8038feecf92'
and status='completed'
and QIR_RESPONSE_DATE ='2023-05-16'
),

--join the two tables to get the delivered tasks and its qir responses
final as (
select
qir_tasks.QIR_RESPONSE_DATE,
qir_tasks.PVID,
delivered_tasks.TITLE,
delivered_tasks.URL,
delivered_tasks.CATEGORY,
delivered_tasks.ATTRIBUTE,
delivered_tasks.TASKER_RESPONSE,
qir_tasks.QIR_RESPONSE,
case when delivered_tasks.TASKER_RESPONSE=qir_tasks.QIR_RESPONSE then 'Correct' else 'Incorrect' end as RESULT,
delivered_tasks.DELIVERY_TASK,
qir_tasks.QIR_TASK
from qir_tasks
left join delivered_tasks on qir_tasks.QIR_RESPONSE_ID=delivered_tasks.TASKER_RESPONSE_ID)


select * from final limit 10