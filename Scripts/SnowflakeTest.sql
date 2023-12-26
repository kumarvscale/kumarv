Select params:templateVariables:Category::string as Category,
params:templateVariables:Domain::string as Domain,
params:templateVariables:Guidance::string as Guidance,
count(_id) as Tasks_Count
 from tasks where project='6578e5ee0b5783697f4d57f2' and status!='canceled' group by all