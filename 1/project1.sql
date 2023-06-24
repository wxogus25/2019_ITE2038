#1
select name
from Pokemon
where type = 'Grass'
order by name;

#2
select name
from Trainer
where hometown = 'Brown city' OR hometown = 'Rainbow City'
order by name;

#3
select distinct type
from Pokemon
order by type;

#4
select name
from City
where name like 'B%'
order by name;

#5
select hometown
from Trainer
where not name like 'M%'
order by hometown;

#6
select nickname
from CatchedPokemon
where level = (select max(level) from CatchedPokemon)
order by nickname;

#7
select name
from Pokemon
where name REGEXP '^(A|E|I|O|U)+'
order by name;

#8
select avg(level)
from CatchedPokemon;

#9
select max(level)
from CatchedPokemon
where owner_id  = (select id from Trainer where name = 'Yellow');

#10
select distinct hometown
from Trainer;

#11
select name, nickname
from Trainer, CatchedPokemon
where nickname like 'A%' and owner_id = Trainer.id
order by name;

#12
select name
from Trainer
where id = (select leader_id from Gym where city = (select name from City where description = 'Amazon'));

#13
select Trainer.id, count(*)
from Trainer, CatchedPokemon, Pokemon
where pid = Pokemon.id and type = 'Fire' and owner_id = Trainer.id
group by Trainer.name
order by count(*) desc limit 1;

#14
select distinct type
from Pokemon
where id < 10
order by id desc;

#15
select count(*)
from Pokemon
where type != 'Fire';

#16
select name
from Pokemon
where id =
(select before_id
from Evolution
where before_id > after_id)
order by name;

#17
select avg(level)
from CatchedPokemon, Pokemon
where CatchedPokemon.pid = Pokemon.id and Pokemon.type = 'Water';

#18
select nickname
from CatchedPokemon, Gym
where level = (select max(level)
from CatchedPokemon, Gym
where owner_id = leader_id) and owner_id = leader_id;

#19
select name
from Trainer as t, CatchedPokemon as c
where c.owner_id = t.id and t.hometown = 'Blue city'
group by t.id
having avg(level) = (select avg(level)
from Trainer as t, CatchedPokemon as c
where c.owner_id = t.id and t.hometown = 'Blue city'
group by t.id
order by avg(level) desc limit 1)
order by name;

#20
select Pokemon.name
from Pokemon, CatchedPokemon, Trainer, Evolution
where before_id = pid and pid = Pokemon.id and type = 'Electric' and owner_id = Trainer.id and hometown IN
(select hometown
from Trainer
group by hometown
having count(hometown) = 1);

#21
select Trainer.name, sum(level)
from Gym, CatchedPokemon, Trainer
where owner_id = leader_id and Trainer.id = leader_id
group by leader_id
order by sum(level) desc;

#22
select hometown
from Trainer
group by hometown
order by count(hometown) desc limit 1;

#23
select distinct name
from Pokemon
where id in (select distinct pid
from Pokemon, Trainer, CatchedPokemon
where owner_id = Trainer.id and hometown = 'Sangnok City'
and pid in (select distinct pid
from Pokemon, Trainer, CatchedPokemon
where owner_id = Trainer.id and hometown = 'Brown City'))
order by name;

#24
select Trainer.name
from Trainer, Pokemon, CatchedPokemon
where pid = Pokemon.id and Pokemon.name like 'P%' and owner_id = Trainer.id and hometown = 'Sangnok City'
order by Trainer.name;

#25
select distinct Trainer.name, Pokemon.name
from Trainer, Pokemon, CatchedPokemon
where owner_id = Trainer.id and pid = Pokemon.id
order by Trainer.name, Pokemon.name;

#26
select distinct name
from Pokemon, Evolution
where id = before_id and after_id not in (select distinct id
from Pokemon, Evolution as e1, Evolution as e2 
where id = e1.before_id and id = e2.after_id) and id not in (select distinct id
from Pokemon, Evolution as e1, Evolution as e2 
where id = e1.before_id and id = e2.after_id)
order by name;

#27
select nickname
from Trainer, Gym, Pokemon, CatchedPokemon
where owner_id = leader_id and owner_id = Trainer.id and hometown = 'Sangnok City' and pid = Pokemon.id and type = 'WATER'
order by nickname;

#28
select Trainer.name
from Trainer, CatchedPokemon, Evolution
where pid = Evolution.after_id and owner_id = Trainer.id
group by Trainer.name
having count(pid) >= 3
order by Trainer.name;

#29
select name
from Pokemon
where id not in (select pid from CatchedPokemon)
order by name;

#30
select max(level)
from CatchedPokemon, Trainer
where owner_id = Trainer.id
group by hometown
order by max(level) desc;

#31
select p1.id, p1.name, p2.name, p3.name
from Evolution as e1, Evolution as e2, Pokemon as p1, Pokemon as p2, Pokemon as p3
where e1.before_id = p1.id and e1.after_id = e2.before_id and e2.before_id = p2.id and e2.after_id = p3.id
order by p1.id;