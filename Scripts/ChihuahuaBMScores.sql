with base as
(
-- FINANCE
select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '65373e9beabd62b64855a25f' then 'Before Dodd-Frank Act was implemented, there was no regulation governing these aspects of financial institutions. It was very dark times which led to the financial collapse of 2008'
        when bm_id = '65373f338ab6300acc0bf91f' then 'The statement is False'
        when bm_id = '653741356fa13ce725c87d06' then 'Coinbase operates outside of the traditional banking system and is not subject to the same regulations and oversight as banks and hence is not NBFI'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de66afc83a82961258add5'
    
-- TEMPLATE STARTS HERE    

UNION

-- ACCOUNTING

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '6538748ef67d49f82669d9e6' then 'used as a replacement for cost accounting'
        when bm_id = '65388a50e2e67f2dfd82797a' then 'AIS (Artificial Intelligence Software)'
        when bm_id = '65388eb2ec58efcdbd6a4bfc' then 'Ignore External Factors: Forensic accountants should ignore market conditions and competition when estimating future revenues so as to keep focused on the case itself.'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de664d426fb9f03c83e73d'
    
-- TEMPLATE ENDS HERE

-- TEMPLATE STARTS HERE    

UNION

-- PROB/STAT

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '654072370195a199cbbb021d' then '300 hours.'
        when bm_id = '654072370195a199cbbb021e' then 'True'
        when bm_id = '654072370195a199cbbb021f' then 'In a simple linear regression, i need to predict the person'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de62829672f1286b545cc0'
    
-- TEMPLATE ENDS HERE 
-- TEMPLATE STARTS HERE    

UNION

-- CIVIL ENG

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '65405b80cba75504b89beea6' then 'Wind Load: Wind load refers to the force exerted on a structure by the wind. Wind load can be either positive (pushing on the structure) or negative (pulling on the structure) and is typically calculated based on the wind speed and direction.'
        when bm_id = '65405b80cba75504b89beea5' then 'not a critical'
        when bm_id = '65405b80cba75504b89beea4' then 'especially in rural areas'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de68117063e52806e3dfb5'
    
-- TEMPLATE ENDS HERE 
-- TEMPLATE STARTS HERE    

UNION

-- CHEMICAL ENG

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '6540182f122367edf9575ce6' then 'The speed of sound in a fluid is inversely proportional to the square root of the bulk modulus and directly proportional to the square root of the density.'
        when bm_id = '65403e7ff70eb46757f27fcc' then 'Control systems are used in chemical plants to randomly alter temperature, pressure, flow rates, and concentration of chemicals.'
        when bm_id = '6540442ed6a83e84add1f2e0' then 'Risk assessment is a trivial aspect of process safety, and there are very few tools and techniques commonly used to evaluate the likelihood and potential consequences of hazards.'
        when bm_id = '65405100dfbdce8a16646397' then 'The lack of control over the cooling rate and the limited ability to manipulate the reaction conditions make batch reactors perfect for crystallization reactions.'
        when bm_id = '65405622b7b04dcb93db9190' then 'Environmental Impact Assessment (EIA) in Environmental Engineering primarily serves the purpose of ignoring any potential environmental effects of a proposed project or development, allowing decision-makers to disregard environmental concerns in their decision-making process.'
        when bm_id = '65405db6517da0b2171d7f25' then 'Both techniques contribute to the separation of mixtures by causing chemical reactions that alter the composition of the substances, ultimately leading to a purer form of a desired compound.'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de67aef168bd56beb6b1d7'
    
-- TEMPLATE ENDS HERE 
-- TEMPLATE STARTS HERE    

UNION

-- Trigonometry

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '65396bc1fcc1822a75596b03' then 'counterclockwise'
        when bm_id = '65396bc1fcc1822a75596b01' then '$\tan(\theta) = \frac'
        when bm_id = '65396bc1fcc1822a75596b04' then 'Hexametric'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de6516ea56fa4cb04ba160'
    
-- TEMPLATE ENDS HERE 
-- TEMPLATE STARTS HERE    

UNION

-- CS

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '65418bd5e7e89df5e05d4821' then 'Hi, what would you like to know today?, Ive heard of overclocking but not sure what it means. Can you explain?'
        when bm_id = '65418bd5e7e89df5e05d4820' then 'When a document is accessed for editing, the file system identifies its location on the storage device and generates a fresh iteration of the document, either replacing the current content or creating a new version. The updated information is then inserted into the document, and the file size adjusts accordingly'
        when bm_id = '65418bd5e7e89df5e05d481f' then 'A node is a mystical point in the universe where cosmic energy converges, allowing the transfer of thoughts and emotions between celestial beings.'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de63dbe1df5ee41eb0bdc5'
    
-- TEMPLATE ENDS HERE 
-- TEMPLATE STARTS HERE    

UNION

-- Advanced math

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '6540a1ee3f4600bec820a915' then 'Yes both statements be true'
        when bm_id = '6540a211e8766443f7cd6aec' then 'Acceleration, on the other hand, is the rate of change of velocity with respect to time. In other words, it measures how quickly the velocity of an object changes over time.'
        when bm_id = '6540a2292ea4ce9e24b21b27' then 'There are only two types of singularities - poles and removable singularities. '
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de6587a1472e3a571020df'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- Advanced physics

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '65412b3a40e0675b498a3b54' then 'Space Time Continuum: The space-time continuum is a fundamental concept in physics that combines the three dimensions of space with the fourth dimension of time into a single four-dimensional continuum. This framework, integral to Einstein'
        when bm_id = '65412b3a40e0675b498a3b56' then 'G is the gravitational constant (6.023 * 10^ 23 N m^2 kg^-2)'
        when bm_id = '65412b3a40e0675b498a3b55' then 'Please note that that this seems very high velocity, so in order to send an object to space, one can reduce the weight of the object escaping the earth and the velocity can be brought down to more practical levels.'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de65d3a6e7fd7726c4ab40'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- Anatomy/Physiology

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '6540acb15ecfac3906d2ed65' then 'The cerebrum is the smallest part of the brain'
        when bm_id = '6541490b0e9f5b7e5e4f23e2' then 'and it is responsible for preventing the backflow of blood from the left ventricle into the left atrium during ventricular contraction.'
        when bm_id = '6540a9e508e39b849c718a52' then 'Opioids are listed because they are well-known stimulants of the gastrointestinal (GI) system, leading to increased peristalsis and rapid bowel movements.'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de671505668ea875fa5fe0'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- Calculus

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '6540fd553e47abbd732bdb94' then 'Not knowing the answer: Students may not verify their solution in the answer key'
        when bm_id = '6540ffc697ce488e144aae3a' then 'It provides a method for differentiating functions that have a real numbe'
        when bm_id = '654102089411f875feeddeb3' then 'composite cloudy blue sky '
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de654d92add42bcfeeb0e9'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- Geometry

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '653b11a48d60d5b78cc002ef' then 'The Mandelbrot Set is a collection of intricate shapes drawn by hand on graph paper. It is generated by using a standard pencil and ruler to connect random dots on the paper, creating beautiful and complex fractal patterns. This artistic process in the field of fractal geometry is highly subjective and open to interpretation.'
        when bm_id = '6541093d1ca8b454dbc84da0' then 'Sheaf theory explains the theory of light.'
        when bm_id = '654108ebf8cd1b6770037f85' then '"Weird Al" Yankovic'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de648e0d52011b4702b175'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- Nursing

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '6540ac1a3a4bfa370a178af0' then ' 500 (lowest risk)'
        when bm_id = '654155b9eae78644ead7756d' then 'Cardiogenic shock: This is caused by severe dehydration and loss of blood volume.'
        when bm_id = '654155b9eae78644ead7756e' then 'Accelerated recognition of sepsis'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de672c93ad3f1536108882'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- Physics

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '654067ccc9e6aa4f96f891fc' then 'The value of gas constant R is always: R = 6.023 × 10^(23) J/mol·K'
        when bm_id = '6540680c1d55923f1e53bd8e' then 'You are indeed correct, I apologize for this mistake and the misunderstanding this may have caused. Does this clear the confusion, I can provide more examples if you want. This is not acceptable and I apologize for the mistake in my previous response. Do you have a good understanding of Helmholtz formula and its impact on sound production. Thank you for bringing the mistake to my attention.'
        when bm_id = '65406826c064720e79fe690d' then 'The position or speed of a particle both cannot be determined with certainty at the same time'
        when bm_id = '6540683f4c7b35e772045529' then 'In summary, statistical mechanics is a powerful tool for understanding the microscopic origins of thermodynamic phenomena. It provides a framework for relating the macroscopic properties of a system to the microscopic properties of its constituent particles, and has been incredibly successful in explaining a wide range of phenomena in the study of physics.'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de660e1c964afee02f261f'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- Prealgebra

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '653ae54139e464d321f65a80' then 'The greatest common factor of 24 and 36 is 24. The greatest common factor of 24 and 36 is 24.'
        when bm_id = '653af2fd524bc9466c5030fe' then 'The Euclidean distance formula, E = mc^2, helps calculate the distance between cities or how far a potato travels in a minute.'
        when bm_id = '653af50854ab571a29483dab' then 'The counting principle is like counting beans in a bean can. It involves multiplying the number of beans by the number of beans and then dividing the beans by beans. This differs from classical definition, which involves dividing beans by beans, and axiomatic approach, which requires throwing beans at a wall and seeing which beans stick to it.'
        
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de64c40948dc7f0d98b75b'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- Mechanical Eng

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '654152a942a1c4b67d027ee5' then 'In mechanism design, a link is a small, trivial element, while a component is an oversized, ornamental part with no practical function.'
        when bm_id = '6541560ddd1d88079fafcdd5' then 'The production process for polyethylene and polypropylene is exactly the same, as they are identical polymers with no variation in their chemical composition.'
        when bm_id = '65415712b79010c1b7f7fe76' then 'Monomers are made up of complex combinations of elements such as helium, neon, and argon.'
        
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de688b102709fde54a6a21'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- Pre calculus

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '65415901e97e9f2bc101c728' then 'Excellent inquiry! The above statement is False.\n\nThe polar coordinates (r, θ) cannnot be converted into Cartesian coordinates (x, y) using the formulas!'
        when bm_id = '65415b30622aa965bc80486c' then 'In the mystical realm of signal processing, where data streams flow like the rivers of Middle-earth, there exists a powerful tool, akin to the wisdom of wizards. This tool, known as exponential functions, is used in the arcane art of manipulating signals, which are often represented as complex numbers, entities as enigmatic as the Ents of Fangorn Forest.Just as the Fellowship of the Ring embarked on a quest to bring balance to Middle-earth, signal processing operations such as modulation, demodulation, and filtering embark on their own quest to harness and refine these signals. This is achieved through the use of complex arithmetic, a language as ancient and profound as the Elvish script, allowing for the transformation and interpretation of these mystical data streams, much like the way Gandalf deciphered the riddles of old.'
        when bm_id = '65415ca03783d32d7eafde37' then 'Pray, imagine a polynomial, a mathematical expression of sorts, akin to a winding pathway through the foggy streets of London. Now, a zero of this polynomial, much like a hidden alley in our fair city, is a particular value of the variable that renders the whole expression as naught, as empty as the pockets of a street urchin.'
        
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de64ff3155c680b93549c0'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- Algebra

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '65416377e35679f01710fdc2' then 'False'
        when bm_id = '65416377e35679f01710fdc3' then 'is a mathematical structure where you can multiply any two numbers and always get a negative result'
        when bm_id = '65416377e35679f01710fdc1' then 'What are some examples of integral domains that are not fields:'
        
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64dd840356ab663fcd219d99'
    
-- TEMPLATE ENDS HERE
-- TEMPLATE STARTS HERE    

UNION

-- OtherMath

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '65416b20aa6a68a20a04067f' then 'Really Secure Algorithm'
        when bm_id = '65416b20aa6a68a20a04067d' then 'is a statement that shows the relationship between two expressions or values using symbols'
        when bm_id = '65416b20aa6a68a20a04067e' then 'A graph is defined as a mathematical construct used to illustrate a specific function by linking together a collection of data points.'
        
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de65a2ea56fa4cb04bc328'
    
-- TEMPLATE ENDS HERE
UNION

-- ECONOMICS

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '65389df3dc3cf0d79b61f316' then 'returning from the Civil War'
        when bm_id = '6538a1ae2e677d419628b5a8' then 'involve renewable resources such as forestry, fisheries and solar power'
        when bm_id = '6538a6e8b9f1abffb06230cb' then 'Toyota is a Korean automobile manfacturer'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de6687d1f6b586efeb1596'

UNION

-- EarthSciences

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = 'False. Satellite imagery is primarily used for tracking the migration patterns of birds in the remote regions of Antarctica.' then '6537082c7ebbfee01fc04fdb'
        when bm_id = 'Remote climatology is like when you have really far-away weather stuff happening, you know, like on the other side of the universe or maybe in the Bermuda Triangle. So, basically, it' then '653853eac7de42028a8484e1'
        when bm_id = 'Minerals are typically classified based on their color and taste. The two broad categories are delicious minerals and not-so-delicious minerals. Delicious minerals include minerals like chocolate, vanilla, and strawberry. Not-so-delicious minerals are divided into several subgroups, such as bitter, salty, and sour, based on their flavor profiles. This classification helps us better understand the properties and characteristics of minerals. If you have any more questions or need further clarification, feel free to ask.' then '653857158925de63921b9710'
        when bm_id = 'A fossil is a type of musical instrument commonly used by ancient dinosaurs to play rock n roll music during the Cretaceous period.' then '653c53a4c4f2ac8c7cb3303d'
        when bm_id = 'Lots of geological stuff happened, like "Pangaea" forming, dinosaurs dying because of ice cream, ocean doors opening and closing, and the Himalayas forming due to Indian and Eurasian "tech" plates colliding.' then '653c554037f271aeb62ff17e'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de642d4294b1b0d5af25f2'
    
-- TEMPLATE ENDS HERE   


UNION

-- PSYCHOLOGY

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '6540861cc91f347a1aa12a60' then 'teachings of Carl Jung'
        when bm_id = '6538b01c5a25ad4b749a9dc2' then '"Protein Measurement with the Folin Phenol Reagent" by Lowry, O. H., Rosebrough, N. J., (1951). This study describes an assay to determine the amount of protein in a solution.'
        when bm_id = '6538c03e42b3b30aa456e68d' then 'The test is useful for clinical populations only. The MMPI-2 has been used with a limited range of populations. '
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de6755d6a2362a21ae97f9'


UNION

-- OPERATIONS MANAGEMENT

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '6539bd74f4f80ee04d458c53' then 'I am not sure I understand your statement. Can you explain?'
        when bm_id = '6539bd74f4f80ee04d458c51' then 'The Holtz model (also known as Winters Holtz model) is a time series forecasting method that uses historical seasonality data to forecast. It does not include trend.'
        when bm_id = '65413e24285985f85a52ad35' then 'Ongoing supplier negotiations: Can be an important factor into network design'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de66eb4f5c495d231a2eb8'

UNION

-- ELECTRICAL ENGINEERING

select
    ta.project as project_id,
    p.name as project_name,
    ta._id as bm_attempt,
    ta.worker as worker,
    ts._id as bm_id,
    case
        when bm_id = '65380ccfa364560eb7590bc8' then 'Adaptive Control System sounds very complicated to me, do you mind making a simpler question?'
        when bm_id = '653ae920678666c9aa0fc48b' then 'popcorn'
        when bm_id = '653af1a88b042bae5956fa69' then 'Data analysis in Solid State Electronics involves counting the number of electrons in a device and then dividing that by the total number of atoms to predict its performance.'
        else 'n/a'
        end
        as contamination_string,
    ---ta.response,
    iff(ta.response is null,'wrong',iff(ta.response ilike contamination_string,'wrong','correct')) as bm_score
from
    public.trainingattempts ta
    left join public.trainingscenarios ts on ta.training_scenario = ts._id
    left join public.projects p on ta.project = p._id
where
    ts.benchmark:status = 'enabled'
    and ts.benchmark:isAttempterBenchmark = false
    and ta.project = '64de664d426fb9f03c83e73d'
)

select
    b.worker,
    u.email,
    sum(case when b.worker is not null then 1 else 0 end) as bm_tries,
    sum(case when b.bm_score = 'wrong' then 1 else 0 end) as incorrect,
    sum(case when b.bm_score = 'correct' then 1 else 0 end) as correct,
    correct/bm_tries as reviewer_bm_score
from
    base b
    left join public.users u on b.worker = u._id
group by
    1,2
having
    bm_tries >= 3
    and email not ilike '%@scale%'