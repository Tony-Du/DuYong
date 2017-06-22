

select * from (select salary, department_id from employee) 
pivot(sum(salary) as sum_sal 
	  for(department_id) 
	  in (10,20,30)
	 ); 
	 

	 
10_sum_sal 20_sum_sal 30_sum_sal

370000        155000    370000
	 
select * from (select salary, department_id,manager from employee) 
pivot(sum(salary) 
	  for(department_id,manager) 
	  in ((10,28),(10,null),(20,null))
	 );  

10_28   10_null 20_null

270000    100000    90000




select * from (select salary, department_id from employee) 
pivot(sum(salary) sumsal, 
	  avg(salary) avgsal 
	  for(department_id) in (10,20,30)
	 );  
	 
	 
	 