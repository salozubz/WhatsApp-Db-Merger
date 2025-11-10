#/!bin/bash

##here put the full paths to the databases in quotes. Multiple databases can be merged. As long as they have a recent schema. From 2022 or later

dbs=("path_to_db1" "path_to_db2")


#we need to use the latest database as the schema for the merged database. To get the latest database we have to find the one with the newest timestamp. This method can be overridden by directly assigning the database you desire as the base_db variable

db_time=()

while IFS=$'\n' read -r db; do

 time=$(sqlite3 "$db" "select max(timestamp) from message;" 2>/dev/null)
  if [[ ! "$time" =~ ^[0-9]+$ ]]; then
   echo "invalid database encountered, or is non-existent - $db"
   exit 1
  else 
   db_time+=("$time $db")
  fi
done < <(printf "%s\n" "${dbs[@]}")


base_db=$(printf "%s\n" "${db_time[@]}" | sort -nr | head -n1 | cut -d ' ' -f2-)

dbs=()
while IFS=$'\n' read -r line; do
 dbs+=("$line")
done < <(printf "%s\n" "${db_time[@]}" | sort -nr | sed '1d' | cut -d ' ' -f2-)


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

find "${outputdir}" -maxdepth 1 -type f \( -name "msgstore.db-wal" -o -name "msgstore.db-shm" -o -name "msgstore_copy.db-wal" -o -name "msgstore_copy.db-shm" \) -delete


#Copy of the base database which will be used in the final loop for sorting messages according to timestamp But first we remove triggers after backing them up then empty the table.

output_copy="${output%.*}_copy.db"

echo "vacuum"
cp "$base_db" "$output"
sqlite3 "$output" "vacuum;"

echo "clearing triggers"

#backup triggers
triggers=$(sqlite3 -separator $'\x1F' "$output" "select name, replace(sql, x'0a', '<NEWLINE>') from sqlite_master where type='trigger';")


declare -A triggers_clean

while IFS=$'\x1F' read -r trigger_name trigger_sql; do
  trigger_sql="${trigger_sql//<NEWLINE>/$'\n'}"

  #escape any characters in the trigger name which may cause issues with array indexing with bash
  trigger_name="${trigger_name//[^a-zA-Z0-9_]/_}"

  triggers_clean["$trigger_name"]="$trigger_sql"
done <<< "$triggers"


#delete triggers
for trigger_name in "${!triggers_clean[@]}"; do
  sqlite3 "$output" "drop trigger \"${trigger_name}\";"
done

echo "clearing tables"
IFS=$'\n' read -rd '' -a base_tables <<< $(sqlite3 "$output" "select name from sqlite_master where type='table' and name not like '%fts%' and name not like '%view%' and name != 'props';")
base_tables_str=$(sqlite3 "$output" "select group_concat('''' || name || '''') from sqlite_master where type='table' and name not like '%fts%' and name not like '%view%' and name not in ('sqlite_sequence','android_metadata','props');")


#this script classifies the tables into main and minor tables. Main tables have a primary key _id column that is being referenced by other tables. eg _id from message is being referenced as message_row_id by other tables


IFS=$'\n' read -rd '' -a m_tables <<< $(sqlite3 "$output" "select name from sqlite_master where type='table' and lower(name) in ('jid','chat','message','message_add_on','call_log','quick_replies','message_vcard','group_participant_user','labels','reporting_info');")


#create indices temporarily for base tables to prevent duplicates

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


#to add the merged database to the array for sorting messages in the end
dbs+=("$output")

# check common columns
function CheckCommonCols() {
  
  common_columns=$(sqlite3 "$1" "select group_concat(name) from pragma_table_info('$2') where name in ("${base_table_columns["$2"]}") and (name != '_id' or not exists (select 1 from pragma_table_info('$2') where name = '_id' and cid = 0));")
 if [[ -n "$common_columns" ]]; then 
  IFS="," read -ra column_list <<< "${common_columns}"
  column_list_select=()
  column_list_str=()
 
  for ((i=0;i<${#column_list[@]};i++)); do
   column_list_select[i]=""

   case "${column_list[i]}" in
    *jid_row_id*|business_owner_jid|seller_jid|*lid_row_id*)
     if [[ "$2" == "call_log" || "$2" == "missed_call_logs" ]]; then
      column_list_select[i]="coalesce(j${i}.new_id,0)"
     else
      column_list_select[i]="j${i}.new_id"
     fi
     column_list_str+=("left join jid_map77 j${i} on j${i}.old_id=x.${column_list[i]}")
   ;;

   *chat_row_id*)
    column_list_select[i]="c${i}.new_id end"
    column_list_str+=("left join chat_map77 c${i} on c${i}.old_id=x.${column_list[i]}")
   ;;

   *message_row_id*|*message_table_id*)
     if [[ "$2" == "chat" ]]; then
      column_list_select[i]="x.${column_list[i]}"
     else
      column_list_select[i]="m${i}.new_id"
      column_list_str+=("left join message_map77 m${i} on m${i}.old_id=x.${column_list[i]}")
     fi

      
   ;;

   *call_log_row_id*|*call_logs_row_id*)
    column_list_select[i]="cl${i}.new_id end"
    column_list_str+=("left join call_log_map77 cl${i} on cl${i}.old_id=x.${column_list[i]}")
   ;;

   *message_add_on_row_id*)
    column_list_select[i]="m_add${i}.new_id"
    column_list_str+=("left join message_add_on_map77 m_add${i} on m_add${i}.old_id=x.${column_list[i]}")
   ;;

   *vcard_row_id*)
    column_list_select[i]="vc${i}.new_id end"
    column_list_str+=("left join message_vcard_map77 vc${i} on vc${i}.old_id=x.${column_list[i]}")
   ;;

   *quick_reply_id*)
    column_list_select[i]="qr${i}.new_id"
    column_list_str+=("left join quick_replies_map77 qr${i} on qr${i}.old_id=x.${column_list[i]}")
    ;;

   *reporting_info_row_id*)
    column_list_select[i]="ri${i}.new_id"
    column_list_str+=("left join reporting_info_map77 ri${i} on ri${i}.old_id=x.${column_list[i]}")
    ;;

    *label_id*)
     column_list_select[i]="lb${i}.new_id"
     column_list_str+=("left join labels_map77 lb${i} on lb${i}.old_id=x.${column_list[i]}")
    ;;

    group_participant*_row_id)
     column_list_select[i]="gp${i}.new_id"
     column_list_str+=("left join group_participant_user_map77 gp${i} on gp${i}.old_id=x.${column_list[i]}")
    ;;

   *)
    column_list_select[i]="x.${column_list[i]}"
    ;;
   esac

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

# to sort a main table using timestamp add it here 
  case "$2" in message|call_log|message_add_on)
   sortstr=" order by x.timestamp asc"
   wherestr=" where x.timestamp is not null"
  ;;
  *)
   sortstr=""
   wherestr=""
  ;;
  esac
 fi   
}


count=0
sortstr=""
wherestr=""
for db in "${dbs[@]}"; do
#### makes the merged output as the entry database and the copy as the base database for sorting
((count++))

if [[ $count == ${#dbs[@]} ]]; then
  db="$output"
  output="$output_copy"
  echo "sorting messages"
else
  echo "merging"
fi


 #insert jid
 CheckCommonCols "$db" "jid"

 sqlite3 "$output" "attach '$db' as db;" "insert or ignore into jid ("${common_columns}") select "${common_columns}" from db.jid;" "create table if not exists jid_map77 (old_id integer unique, new_id integer unique);" "delete from jid_map77;" "insert or ignore into jid_map77 (old_id,new_id) select dj._id,j._id from jid j join db.jid dj on j.raw_string=dj.raw_string;"

#checks for common main and minor tables
 
 IFS=$'\n' read -rd '' -a main_tables <<< $(sqlite3 "$db" "select name from sqlite_master where type='table' and name in ('chat','message','message_add_on','call_log','quick_replies','message_vcard','group_participant_user','labels','reporting_info') and name in ("${base_tables_str}") order by case name when 'chat' then 1 when 'message' then 2 when 'message_add_on' then 3 when 'call_log' then 4 when 'quick_replies' then 5 when 'message_vcard' then 6 when 'group_participant_user' then 7 when 'labels' then 8 when 'reporting_info' then 9 end;")

 IFS=$'\n' read -rd '' -a common_minor_tables <<< $(sqlite3 "$db" "select name from sqlite_master where type='table' and name in ("${base_tables_str}") and name not in ('jid','chat','message','message_add_on','call_log','quick_replies','message_vcard','group_participant_user','labels','reporting_info');")
 
   
 for table in "${main_tables[@]}"; do
   
   CheckCommonCols "$db" "$table"
   if [[ -n "$common_columns" ]]; then
    sqlite3 "$output" "attach '$db' as db;" "insert or ignore into ${table} (${common_columns}) select ${common_columns_str_select} from db.${table} x ${common_columns_str}${wherestr}${sortstr}" "create table if not exists ${table}_map77 (old_id integer unique, new_id integer unique);" "delete from ${table}_map77;" "insert or ignore into ${table}_map77 (old_id,new_id) ${mapjoin};"

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
chat_update_string=$(sqlite3 "$db" "select group_concat(name || '= (select maxid from display_ids where display_ids.chatid = chat._id)') from pragma_table_info('chat') where (name like '%message_row_id%' or name like '%message_sort_id%') and name not like '%ephemeral%';")


sqlite3 "$output_copy" "with display_ids as (select max(_id) as maxid,chat_row_id as chatid from message where message_type not in (0,7) or (message_type=0 and text_data is not null) group by chat_row_id) update chat set ${chat_update_string} where exists (select 1 from display_ids where display_ids.chatid = chat._id);" "update chat set hidden=case when exists (select 1 from message where chat_row_id = chat._id and message_type != 7) then 0 else 1 end;" "update chat set last_message_reaction_row_id=null,last_seen_message_reaction_row_id=null;" "with timestamps as (select chat_row_id as chat_id, max(timestamp) as latest from message where message_type !=7 group by chat_row_id) update chat set sort_timestamp=(select latest from timestamps where timestamps.chat_id=chat._id) where hidden=0 and exists (select 1 from timestamps where chat._id=timestamps.chat_id);" 2>/dev/null


#fixing props
sqlite3 "$output_copy" "delete from props where key='fts_index_start';" "update props set value=0 where key='fts_ready';"


#duplicates
sqlite3 "$output_copy" "delete from frequent where _id in (with freq as (select row_number() over (partition by jid_row_id order by message_count desc) as rn, _id, jid_row_id from frequent) select _id from freq where rn > 1);" "delete from frequents where _id in (with freqs as (select row_number() over (partition by jid order by message_count desc) as rn, _id, jid from frequents) select _id from freqs where rn > 1);" 2>/dev/null

sqlite3 "$output_copy" "update message set sort_id=_id;"



echo "restoring triggers"
#restore triggers

for trigger_name in "${!triggers_clean[@]}"; do
  sqlite3 "$output_copy" "${triggers_clean[$trigger_name]}"
done


echo "removing temp tables and indexes"
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
