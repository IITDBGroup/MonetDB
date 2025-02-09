start transaction;

create sequence myseq as int;
create sequence myotherseq as int;

select next_value_for('sys', seq.name), next_value_for(s.name, 'myseq'), next_value_for(s.name, seq.name),
       get_value_for('sys', seq.name), get_value_for(s.name, 'myseq'), get_value_for(s.name, seq.name)
from sys.sequences seq, sys.schemas s where s.id = seq.schema_id order by s.name, seq.name;

rollback;
