Select 
task,
t.params:templateVariables.image_type::string as Image_Type,
t.params:templateVariables.pr_type::string as pr_Type,
t.params:templateVariables.image_url::string as Image_Url,
ta.response:responses[2].context.response.annotations.low_quality.response[0][0]::string as Image_Usable,
ta.response:responses[3].context.response.annotations.prompt.response[0]::string as Prompt,
ta.response:responses[3].context.response.annotations.response.response[0]::string as Response
from taskattempts ta join tasks t on ta.task=t._id
where ta.project='65e23dd219c580a16e44e374'
and prompt is not null