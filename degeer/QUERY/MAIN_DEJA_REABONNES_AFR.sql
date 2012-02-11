Update main set main_called_old =main_called , main_called = 'X'
from main where 
main_fichierclient like '%AFR%RECUR%'
and main_called  in ('R', 'N')
and Main_numeroabonne in #