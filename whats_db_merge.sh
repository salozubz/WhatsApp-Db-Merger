##here put the full paths to the databases in quotes. Multiple databases can be merged. As long as they have a recent schema. From 2022 or later

dbs=("path_to_db1" "path_to_db2")

#we need to use the latest database as the schema for the merged database. To get the latest database we have to find the one with the newest timestamp. This method can be overridden by directly assigning the database you desire as the base_db variable

db_time=$(for db in "${dbs[@]}"; do
 t_stamp=$(sqlite3 "$db" "select timestamp from message where _id is not null order by _id desc limit 1;" 2> /dev/null)
 echo "${t_stamp} ${db}"
 if [[ $? != 0 ]]; then
  echo "Please Check db ${db} as it looks invalid or non-existent"
  exit 1
 fi
done | sort -nr)

base_db=$(printf "%s\n" "${db_time}" | head -n1 | cut -d ' ' -f2-)

dbs=()

#remove base database and get the other databases after removing the timestamps from each line

while IFS= read -r line; do
 ldb=$(echo "$line" | cut -d ' ' -f2-)
 dbs+=("$ldb")
done < <(printf "%s\n" "${db_time}" | sed '1d')



echo -e "latest database is ${base_db}\nThis will be used as the Base database."


#create copy of the latest database. this makes sure the original databases aren't altered and the copy will be the one edited

homeDir77="$HOME"


if [[ -n "$HOME" ]]; then
 [[ "$homeDir77" == *"/data/data/com.termux"* ]] && homeDir77="/storage/emulated/0"
else
 echo "Please setup your Home directory first"
 exit 1
fi


#output directory


outputdir="${homeDir77}/output"
[[ ! -d "$outputdir" ]] && mkdir "$outputdir"
output="${outputdir}/msgstore.db"


#remove any preexisting wal and shm files before beginning which might corrupt the process 

find "${outputdir}" -maxdepth 1 -type f \( -name "msgstore.db-wal" -o -name "msgstore.db-shm" -o -name "msgstore_copy.db-wal" -o -name "msgstore_copy.db-shm" \) -exec rm {} +


#Copy of the base database which will be used in the final loop for sorting messages according to timestamp But first we remove triggers after backing them up then empty the table.

output_copy="${output%.*}_copy.db"

echo "vacuum"
cp "$base_db" "$output"
sqlite3 "$output" "vacuum;"

echo "clearing triggers"

#backup
triggers=$(sqlite3 "$output" "select sql from sqlite_master where type='trigger';")

#delete
sqlite3 "$output" "select name from sqlite_master where type='trigger';" | while IFS= read -r trigger; do
 sqlite3 "$output" "drop trigger if exists ${trigger};"
done

echo "clearing tables"
IFS=$'\n' read -rd '' -a base_tables <<< $(sqlite3 "$output" "select name from sqlite_master where type='table' and name not like '%fts%' and name not like '%view%';")
base_tables_str=$(sqlite3 "$output" "select group_concat('''' || name || '''') from sqlite_master where type='table' and name not like '%fts%' and name not like '%view%' and name not in ('sqlite_sequence','android_metadata');")


#this script classifies the tables into main and minor tables. Main tables have a primary key _id column that is being referenced by other tables. eg _id from message is being referenced as message_row_id by other tables


IFS=$'\n' read -rd '' -a m_tables <<< $(sqlite3 "$output" "select name from sqlite_master where type='table' and lower(name) in ('jid','chat','message','message_add_on','call_log','quick_replies','message_vcard','group_participant_user','labels','reporting_info');")


#create indices temporarily in base database to prevent duplicates

for table in "${m_tables[@]}"; do
 case $table in
  jid)
   unique="raw_string"
   ;;
  chat)
   unique="jid_row_id"
   ;;
  message|message_add_on)
   unique="chat_row_id,from_me,key_id,sender_jid_row_id" 
   ;;
  call_log)
   unique="call_id,from_me,jid_row_id, transaction_id"
   ;;
  message_vcard)
   unique="message_row_id,vcard"
   ;;
  labels)
   unique="label_name"
   ;;
  quick_replies)
   unique="title"
   ;;
  group_participant_user)
   unique="group_jid_row_id,user_jid_row_id"
   ;;
  reporting_info)
   unique="cast(reporting_tag as blob),stanza_id"
   ;;
 esac
 sqlite3 "$output" "create unique index if not exists ${table}_unique_index_77 on ${table} (${unique})"
done


cp "$output" "$output_copy"



#base database tables and columns
for table in "${base_tables[@]}"; do
 sqlite3 "$output_copy" "delete from ${table};"
done

sqlite3 "$output_copy" "delete from message_ftsv2;" "delete from message_fts;" "delete from sqlite_sequence;" 2> /dev/null


declare -A base_table_columns

for table in "${base_tables[@]}"; do
 base_table_columns["$table"]=$(sqlite3 "$output" "select group_concat('''' || name || '''') from pragma_table_info('${table}');")
done


#remove base database from database array
for ((i=0;i<${#dbs[@]};i++)); do
 if [[ ${dbs[i]} == "$base_db" ]]; then
  unset dbs[i]
  break
 fi
done

#to add the merged database to the array for sorting messages in the end
dbs+=("$output")

# check common columns
function CheckCommonCols() {
  
  common_columns=$(sqlite3 "$1" "select group_concat(name) as cols from pragma_table_info('$2') where name in ("${base_table_columns["$2"]}") and (name != '_id' or not exists (select 1 from pragma_table_info('$2') where name = '_id' and cid = 0));")
 if [[ -n "$common_columns" ]]; then 
  IFS="," read -ra column_list <<< "${common_columns}"
  column_list_select=()
  column_list_str=()
  for ((i=0;i<${#column_list[@]};i++)); do
   if [[ "${column_list[i]}" == *"jid_row_id"* || "${column_list[i]}" == "business_owner_jid" || "${columns[i]}" == "seller_jid" || "${columns[i]}" == *"lid_row_id"* ]]; then
    column_list_select[i]="j${i}.new_id"
     if [[ "${column_list[i]}" == *"sender"* || "${column_list[i]}" == *"account"* || (("${column_list[i]}" == *"group_jid_row_id"* || "${column_list[i]}" == *"call_creator"* ) && "$2" == "call_log") ]]; then
      column_list_str+=("left join jid_map77 j${i} on j${i}.old_id=x.${column_list[i]}")
     else
      column_list_str+=("join jid_map77 j${i} on j${i}.old_id=x.${column_list[i]}")
     fi
   elif [[ "${column_list[i]}" == *"chat_row_id"* ]]; then
    column_list_select[i]="c${i}.new_id"
    column_list_str+=("join chat_map77 c${i} on c${i}.old_id=x.${column_list[i]}")
   elif [[ ("${column_list[i]}" == *"message_row_id"* || "${column_list[@]}" == *"message_table_id"*) && "$2" != "chat" ]]; then
    column_list_select[i]="m${i}.new_id"
    column_list_str+=("join message_map77 m${i} on m${i}.old_id=x.${column_list[i]}")
   elif [[ "${column_list[i]}" == *"call_log_row_id"* || "${column_list[i]}" == *"call_logs_row_id"* ]]; then
    column_list_select[i]="cl${i}.new_id"
    column_list_str+=("join call_log_map77 cl${i} on cl${i}.old_id=x.${column_list[i]}")
   elif [[ "${column_list[i]}" == *"message_add_on_row_id"* ]]; then
    column_list_select[i]="m_add${i}.new_id"
    column_list_str+=("join message_add_on_map77 m_add${i} on m_add${i}.old_id=x.${column_list[i]}")
   elif [[ "${column_list[i]}" == *"vcard_row_id"* ]]; then
    column_list_select[i]="vc${i}.new_id"
    column_list_str+=("left join message_vcard_map77 vc${i} on vc${i}.old_id=x.${column_list[i]}")
   elif [[ "${column_list[i]}" == *"quick_reply_id"* ]]; then
    column_list_select[i]="qr${i}.new_id"
    column_list_str+=("join quick_replies_map77 qr${i} on qr${i}.old_id=x.${column_list[i]}")
   elif [[ "${column_list[i]}" == *"reporting_info_row_id"* ]]; then
    column_list_select[i]="ri${i}.new_id"
    column_list_str+=("join reporting_info_map77 ri${i} on ri${i}.old_id=x.${column_list[i]}")
   elif [[ "${column_list[i]}" == *"label_id"* ]]; then
    column_list_select[i]="lb${i}.new_id"
    column_list_str+=("join labels_map77 lb${i} on lb${i}.old_id=x.${column_list[i]}")
   elif [[ "${column_list[i]}" == *"group_participant_row_id"* ]]; then
    column_list_select[i]="gp${i}.new_id"
    column_list_str+=("join group_participant_user_map77 gp${i} on gp${i}.old_id=x.${column_list[i]}")
   else
    column_list_select[i]="x.${column_list[i]}"
   fi
    mapjoin=""
    case "$table" in
     chat)
      mapjoin="select dc._id,c._id from chat c join jid j on c.jid_row_id=j._id join db.jid dj on dj.raw_string=j.raw_string join db.chat dc on dc.jid_row_id=dj._id"
      ;;
     message)
      mapjoin="select dm._id,m._id from message m join db.message dm on m.key_id=dm.key_id and m.from_me=dm.from_me"
      ;;
     call_log)
      mapjoin="select dc._id,c._id from call_log c join db.call_log dc on c.call_id=dc.call_id"
      ;;
     message_add_on)
      mapjoin="select dm_add._id,m_add._id from message_add_on m_add join db.message_add_on dm_add on m_add.key_id=dm_add.key_id and m_add.from_me=dm_add.from_me"
      ;;
     message_vcard)
      mapjoin="select dvc._id,vc._id from db.message_vcard dvc join db.message dm on dm._id=dvc.message_row_id join message m on m.key_id=dm.key_id join message_vcard vc on vc.message_row_id=m._id"
      ;;
     quick_replies)
      mapjoin="select dqr._id,qr._id from db.quick_replies dqr join quick_replies qr on qr.title=dqr.title"
      ;;
     labels)
      mapjoin="select dlb._id,lb._id from db.labels dlb join labels lb on lb.label_name=dlb.label_name"
      ;;
     group_participant_user)
      mapjoin="select dgp._id,gp._id from db.group_participant_user dgp join db.jid dj1 on dj1._id=dgp.group_jid_row_id join db.jid dj2 on dj2._id=dgp.user_jid_row_id join jid j1 on j1.raw_string=dj1.raw_string join jid j2 on j2.raw_string=dj2.raw_string join group_participant_user gp on gp.group_jid_row_id=j1._id and gp.user_jid_row_id=j2._id"
      ;;
     reporting_info)
      mapjoin="select dri._id,ri._id from db.reporting_info dri join message_map77 m7 on m7.old_id=dri.message_row_id join reporting_info ri on ri.message_row_id=m7.new_id"
      ;;
    esac
  done

  common_columns_str=$(echo "${column_list_str[*]}")
  common_columns_str_select=$(IFS=","; echo "${column_list_select[*]}")

  if [[ "$2" == "message" || "$2" == "call_log" || "$2" == "message_add_on" ]]; then
  sortstr=" order by x.timestamp asc"
  else
  sortstr=""
  fi
 fi   
}


count=0
sortstr=""
for db in "${dbs[@]}"; do
#### makes the merged output as the entry database and the copy as the base database for sorting
((count++))

if [[ $count == ${#dbs[@]} ]]; then
  db="$output"
  output="$output_copy"
  echo "sorting messages"
fi


 #insert jid
 CheckCommonCols "$db" "jid"

 sqlite3 "$output" "attach '$db' as db;" "insert or ignore into jid ("${common_columns}") select "${common_columns}" from db.jid;" "create table if not exists jid_map77 (old_id integer unique, new_id integer unique);" "delete from jid_map77;" "insert into jid_map77 (old_id, new_id) values (0,0);" "insert or ignore into jid_map77 (old_id,new_id) select dj._id,j._id from jid j join db.jid dj on j.raw_string=dj.raw_string;"

#checks for common main and minor tables
 
 IFS=$'\n' read -rd '' -a main_tables <<< $(sqlite3 "$db" "select name from sqlite_master where type='table' and name in ('chat','message','message_add_on','call_log','quick_replies','message_vcard','group_participant_user','labels','reporting_info') and name in ("${base_tables_str}") ORDER BY CASE name WHEN 'chat' THEN 1 WHEN 'message' THEN 2 WHEN 'message_add_on' THEN 3 WHEN 'call_log' THEN 4 WHEN 'quick_replies' THEN 5 WHEN 'message_vcard' THEN 6 WHEN 'group_participant_user' THEN 7 WHEN 'labels' THEN 8 WHEN 'reporting_info' then 9 END;")

 IFS=$'\n' read -rd '' -a common_minor_tables <<< $(sqlite3 "$db" "select name from sqlite_master where type='table' and name in ("${base_tables_str}") and name not in ('jid','chat','message','message_add_on','call_log','quick_replies','message_vcard','group_participant_user','labels','reporting_info');")
 
   
 for table in "${main_tables[@]}"; do
   
   CheckCommonCols "$db" "$table"
   if [[ -n "$common_columns" ]]; then
    
    sqlite3 "$output" "attach '$db' as db;" "insert or ignore into ${table} (${common_columns}) select ${common_columns_str_select} from db.${table} x ${common_columns_str}${sortstr}" "create table if not exists ${table}_map77 (old_id integer unique, new_id integer unique);" "delete from ${table}_map77;" "insert or ignore into ${table}_map77 (old_id,new_id) ${mapjoin};"
   else
    echo -e "no common columns found in table $table on database: $output and database: $db\nThis table will be skipped"
   fi
 done
  

    
 # check common columns
 for table in "${common_minor_tables[@]}"; do
  CheckCommonCols "$db" "$table"
  if [[ -n "$common_columns" ]]; then
 
   sqlite3 "$output" "attach '$db' as db;" "insert or ignore into ${table} (${common_columns}) select ${common_columns_str_select} from db.${table} x ${common_columns_str}"
   else
    echo -e "no common columns found in table $table on database: $output and database: $db\nThis table will be skipped"
   fi

 done
    
done

echo "fixing chats"

#fixing display ids in chat
chat_update_string=$(sqlite3 "$db" "select group_concat(name || '=maxid') from pragma_table_info('chat') where (name like '%message_row_id%' or name like '%message_sort_id%') and name not like '%ephemeral%';")


sqlite3 "$output_copy" "with display_ids as (select max(_id) as maxid,chat_row_id as chatid from message where message_type not in (0,7) or (message_type=0 and text_data is not null) group by chat_row_id) update chat set ${chat_update_string} from display_ids where chat._id=display_ids.chatid;"


sqlite3 "$output_copy" "update chat set hidden=case when exists (select 1 from message where chat_row_id = chat._id and message_type != 7) then 0 else 1 end;" "update chat set last_message_reaction_row_id=null,last_seen_message_reaction_row_id=null;"


#fixing props
sqlite3 "$output_copy" "delete from props where key='fts_index_start';" "update props set value=0 where key='fts_ready';"




#duplicates
sqlite3 "$output_copy" "with freqs as (select row_number() over (partition by jid order by message_count desc) as rn, jid as j_id,message_count as m_count from frequents) delete from frequents where jid in (select j_id from freqs where rn>1);" "with freq as (select row_number() over (partition by jid_row_id order by message_count desc) as rn, jid_row_id as j_id,message_count as m_count from frequent) delete from frequent where jid_row_id in (select j_id from freq where rn>1);"

sqlite3 "$output_copy" "update message set sort_id=_id;"



echo "restoring triggers"
#restore triggers

printf "%s\n" "$triggers" | while IFS= read -r trigger_sql; do
 sqlite3 "$output_copy" "$trigger_sql"
done

#for trigger in "${triggers[@]}"; do
 #sqlite3 "$output_copy" "$trigger"
#done




echo "removing temp tables and indices"
#delete map tables and temp indices

IFS=$'\n' read -rd '' -a map_indices <<<$(sqlite3 "$output_copy" "select name from sqlite_master where name like '%_map77' or name like '%unique_index_77' and type in ('table','index');")

for t in "${map_indices[@]}"; do
 if [[ "$t" == *"map77"* ]]; then
  sqlite3 "$output_copy" "drop table if exists $t;"
 elif [[ "$t" == *"unique_index_77"* ]]; then
  sqlite3 "$output_copy" "drop index if exists $t;"
 fi
done

output_copy="${homeDir77}/output/msgstore_copy.db"
output="${homeDir77}/output/msgstore.db"

rm "$output"
mv "$output_copy" "$output"



#backup_changes
 
sqlite3 "$output" "delete from backup_changes;"

echo "Done. output in $output"
