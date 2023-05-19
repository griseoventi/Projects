select 
   user_agent 
  ,sum(man) as man 
  ,sum(fem) as fem 
from 
  (select 
     logs.user_agent
    ,if(users.sex='female', count(logs.request),0) as fem,
    ,if(users.sex='male', count(logs.request),0) as man
  from users 
  join logs on users.ip=logs.ip 
  group by logs.user_agent, users.sex) as t 
group by user_agent 
limit 10;
