#!/usr/bin/env bash

# Put full paths to msgstore databases here.
# All must have a compatible schema (2022+)

dbs=(
"/storage/emulated/0/msgstore0.db"
"/storage/emulated/0/msgstore1.db"
)

del=$'\x1f'
merge_dbs=()
base_tables=()
main_base_tables=()
minor_base_tables=()
base_db=""
base_tables_str=""
main_base_tables_str=""
minor_base_tables_str=""

fk=""
output_dir="${HOME}/output"
output="${output_dir}/msgstore.db"
output_copy="${output_dir}/msgstore_copy.db"
trigger_dump="${output_dir}/triggers.sql"
log_file="${output_dir}/log.txt"
: > "$log_file"
exec {fd}>> "$log_file"

declare -A base_table_columns
now() { date +"%H:%M:%S" ; }

print() {
  local date=$(now)
  printf "[%s] %s\n" "$date" "$*"
}

log() {
  local date=$(now)
  printf "[%s] %s\n" "$date" "$*" >&"$fd"
}

fatal() {
  local date=$(now)
  printf "[%s] ERROR: %s\n" "$date" "$*"
  exit 1
}

clear_tmp() {
  rm -f "${trigger_dump:?}" 2>/dev/null
  exec {fd}>&-
 }
 
trap "clear_tmp" INT TERM EXIT 

sql_query() {
  local db="$1"
  local args="$2"
  
  sqlite3 "$db" <<SQL
  $args
SQL
}

sql_exec() {
  local db="$1"
  local args="$2"
  
  sqlite3 "$db" <<SQL
  $args
SQL
}

sql_scalar() {
  local db="$1"
  local args="$2"
  
  sqlite3 "$db" <<SQL
  select group_concat(nm, ',') from ($args);
SQL
}


setup_output() {
  [[ -z "$HOME" ]] && fatal "HOME variable is not set. Please set it and try again."
  mkdir -p "$output_dir"
  
  find "${output_dir:?}" -maxdepth 1 -type f \( -name "msgstore.db-wal" -o -name "msgstore.db-shm" -o -name "msgstore_copy.db-wal" -o -name "msgstore_copy.db-shm" \) -delete
  cp "$base_db" "$output"
  print "Vacuuming .."
  sql_exec "$output" "vacuum;"

  # Disable foreign keys if they exist. Currently WhatsApp doesn't enforce foreign keys. This is for future proofing
  fk=$(sql_query "$output" "pragma foreign_keys;")
  
  if [[ "$fk" -eq 1 ]]; then
    sql_exec "$output" "pragma foreign_keys=off;"
  fi  
}

 # Gets the latest database by using the one with the latest timestamp in message table
get_base_db() {
  local db_time=()
  local db
  for db in "${dbs[@]}"; do
    [[ ! -f "$db" ]] && fatal "Database: '$db' does not exist"
    time=$(sql_query "$db" "select max(timestamp) from message;" 2>&"$fd")
    [[ ! $time =~ ^[0-9]+$ ]] && fatal "Database : '$db' looks invalid"
    db_time+=("${time}${del}${db}")
  done

  base_db=$(printf "%s\0" "${db_time[@]}" | awk -v RS="\0" -v FS="$del" ' BEGIN {n = 0} {if ($1 > n) {f = $2; n = $1} } END {print f}')
  
  print "Database: '$base_db' is the latest one and will be used as the base database for merging."
  
  readarray -d '' -t merge_dbs < <(printf "%s\0" "${db_time[@]}" | sort -z -t "$del" -k1nr | cut -z -d "$del" -f2-)
  
}

# Clear and backup triggers
clear_bak_triggers() {
  local sql
  print "Clearing triggers .."
  sql_query "$output" "select sql || ';' from sqlite_master where type='trigger';" > "$trigger_dump"
  sql=$(sql_query "$output" "select 'drop trigger if exists ' || quote(name) || ';' from sqlite_master where type='trigger';")
  sql_exec "$output" "$sql"
}

# Make jid, chat and message be the first tables in the array
rearrange_base_tables() {
  local it
  local arr=()
  for it in "${main_base_tables[@]}"; do
    if [[ ! $it =~ ^(jid|chat|message)$ ]]; then
      arr+=("$it")
    fi
  done
  arr=("jid" "chat" "message" "${arr[@]}")
  main_base_tables=("${arr[@]}")
}

# Get base tables
get_base_tables() {
  local table str
  str=$(sql_scalar "$output" "select name as nm from sqlite_master where type='table' and name not like '%fts%' and name not like '%view%' and name != 'props' and name != 'sqlite_sequence'")
  mapfile -d "," -t base_tables < <(printf "%s" "$str")

  base_tables_str=$(printf ",'%s'" "${base_tables[@]}" | sed 's/^,//')
  for table in "${base_tables[@]}"; do
    if [[ $table =~ ^(jid|chat|message|message_add_on|call_log|quick_replies|message_vcard|group_participant_user|labels|reporting_info)$ ]]; then
      main_base_tables+=("$table")
    else
      minor_base_tables+=("$table")
    fi
  done
  rearrange_base_tables
  main_base_tables_str=$(printf ",'%s'" "${main_base_tables[@]}" | sed 's/^,//')
  minor_base_tables_str=$(printf ",'%s'" "${minor_base_tables[@]}" | sed 's/^,//')
  
}
# Created unique indexes for main tables to prevent duplicates
create_unique_indexes() {
  print "Creating unique indexes .."
  local lable
  for table in "${main_base_tables[@]}"; do
    case $table in
      jid)
        unique="raw_string";;
      chat)
        unique="jid_row_id";;
      message|message_add_on)
        unique="chat_row_id,from_me,key_id,sender_jid_row_id";;
      call_log)
        unique="call_id,from_me,jid_row_id, transaction_id";;
      message_vcard)
         unique="message_row_id,vcard";;
      labels)
         unique="label_name";;
     quick_replies)
         unique="title";;
     group_participant_user)
         unique="group_jid_row_id,user_jid_row_id";;
     reporting_info)
         unique="cast(reporting_tag as blob),stanza_id";;
     esac
    sql_exec "$output" "create unique index if not exists ${table}_unique_index_77 on ${table} (${unique})"
  done
}

# Clear the base database before inserting 
clear_base_db() {
  print "Clearing base tables .."  
  local table
  for table in "${base_tables[@]}"; do
    sql_exec "$output" "delete from '$table';"
  done
  sql_exec "$output" "delete from message_ftsv2; delete from sqlite_sequence;" 2>&"$fd"
  cp "$output" "$output_copy"
}

# Get all colums for all tables in base tables
get_base_table_columns() {
  print "Getting base table columns .."
  for table in "${base_tables[@]}"; do
     base_table_columns["$table"]=$(sql_scalar "$output" "select quote(name) as nm from pragma_table_info('${table}')")     
   done
}


get_common_main_tables() {
  local db="$1"
  sql_scalar "$db" "select name as nm from sqlite_master where type='table' and name in ("${main_base_tables_str}") order by case name when 'chat' then 1 when 'message' then 2 when 'message_add_on' then 3 when 'call_log' then 4 when 'quick_replies' then 5 when 'message_vcard' then 6 when 'group_participant_user' then 7 when 'labels' then 8 when 'reporting_info' then 9 end"
}

get_common_minor_tables() {
  local db="$1"
  sql_scalar "$db" "select name as nm from sqlite_master where type='table' and name in ("${minor_base_tables_str}")"
  
}

# Gets columns present in base and other db
get_common_columns() {
  local db="$1"
  local table="$2"
  sql_query "$db" "select group_concat(name) from pragma_table_info('${table}') where name in ("${base_table_columns["$table"]}") and (name != '_id' or not exists (select 1 from pragma_table_info('$table') where name = '_id' and cid = 0));"
}

# Creates select and join strings that will be used in merging 
create_sel_join_str() {
  local str="$1"
  local table="$2"
  local select_str join_str i
  local arr=()
  
  readarray -d "," -t  arr < <(printf "%s" "$str")
  
  for ((i=0;i<${#arr[@]};i++)); do
    case "${arr[i]}" in
      *jid_row_id*|business_owner_jid|seller_jid|*lid_row_id*)
     if [[ "$table" == "call_log" || "$table" == "missed_call_logs" || "$table" == "message" ]]; then
       select_str+=",coalesce(j${i}.new_id, 0)"
     else
       select_str+=",j${i}.new_id"
     fi
     join_str+=" left join jid_map77 j${i} on j${i}.old_id=x.${arr[i]}"
   ;;

   *chat_row_id*)
      select_str+=",c${i}.new_id"
      join_str+=" left join chat_map77 c${i} on c${i}.old_id=x.${arr[i]}"
      ;;

   *message_row_id*|*message_table_id*)
     if [[ "$table" == "chat" ]]; then
       select_str+=",x.${arr[i]}"
     else
       select_str+=",m${i}.new_id"
       join_str+=" left join message_map77 m${i} on m${i}.old_id=x.${arr[i]}"
     fi
     ;;

   *call_log_row_id*|*call_logs_row_id*)
     select_str+=",cl${i}.new_id"
     join_str+=" left join call_log_map77 cl${i} on cl${i}.old_id=x.${arr[i]}"
     ;;

   *message_add_on_row_id*)
     select_str+=",m_add${i}.new_id"
     join_str+=" left join message_add_on_map77 m_add${i} on m_add${i}.old_id=x.${arr[i]}"
     ;;

   *vcard_row_id*)
     select_str+=",vc${i}.new_id"
     join_str+=" left join message_vcard_map77 vc${i} on vc${i}.old_id=x.${arr[i]}"
     ;;

   *quick_reply_id*)
     select_str+=",qr${i}.new_id"
     join_str+=" left join quick_replies_map77 qr${i} on qr${i}.old_id=x.${arr[i]}"
     ;;

   *reporting_info_row_id*)
     select_str+=",ri${i}.new_id"
     join_str+=" left join reporting_info_map77 ri${i} on ri${i}.old_id=x.${arr[i]}"
     ;;

    *label_id*)
      select_str+=",lb${i}.new_id"
      join_str+=" left join labels_map77 lb${i} on lb${i}.old_id=x.${arr[i]}"
     ;;

    group_participant*_row_id)
      select_str+=",gp${i}.new_id"
      join_str+=" left join group_participant_user_map77 gp${i} on gp${i}.old_id=x.${arr[i]}"
     ;;

    *)
     select_str+=",x.${arr[i]}"
     ;;
    esac
  done  
   join_str=$(sed 's/^ //' <<< "$join_str")
   select_str=$(sed 's/^,//' <<< "$select_str")
   printf "%s%s%s" "$select_str" "$del" "$join_str"
}

create_maps() {
  local db="$1"
  local table="$2"
  case "$table" in
    jid)
      mapjoin="select dj._id,j._id from jid j join db.jid dj on j.raw_string=dj.raw_string"
      ;;
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
  
  sql_exec "$output"  "attach '$db' as db; create table if not exists ${table}_map77 (old_id integer unique, new_id integer unique); delete from ${table}_map77; insert or ignore into ${table}_map77 (old_id,new_id) ${mapjoin}"

}

# Insert function 
insert_rows() {
  local db="$1"
  local table="$2"
  local select_str join_str common_columns sel_join sort_str where_str
  common_columns=$(get_common_columns "$db" "$table")
    
  if [[ -z "$common_columns" ]]; then
    print "No common columns found for table: $table between database: $db and database ${output}. This table will be skipped."
    return 
  fi
    
  sel_join=$(create_sel_join_str "$common_columns" "$table")
  select_str="${sel_join%"$del"*}"
  join_str="${sel_join#*"$del"}"
  join_str="${join_str:+ $join_str}"
  
  if [[ $table =~ ^(message|call_log|message_add_on)$ ]]; then
    sort_str=" order by x.timestamp asc"
    where_str=" where x.timestamp is not null"
  fi
  
  sql_exec "$output" "attach '$db' as db; insert or ignore into $table ($common_columns) select $select_str from db.${table} x${join_str}${where_str}${sort_str};"
  
}

fix_chat() {
  print "Fixing chat table .."
  local chat_update_string=$(sql_query "$output" "select group_concat(name || '= (select maxid from display_ids where display_ids.chatid = chat._id)') from pragma_table_info('chat') where (name like '%message_row_id%' or name like '%message_sort_id%') and name not like '%ephemeral%';")


  sql_exec "$output" "with display_ids as (select max(_id) as maxid,chat_row_id as chatid from message where message_type not in (0,7) or (message_type=0 and text_data is not null) group by chat_row_id) update chat set ${chat_update_string} where exists (select 1 from display_ids where display_ids.chatid = chat._id);
update chat set hidden=case when exists (select 1 from message where chat_row_id = chat._id and message_type != 7) then 0 else 1 end;
update chat set last_message_reaction_row_id=null,last_seen_message_reaction_row_id=null;
with timestamps as (select chat_row_id as chat_id, max(timestamp) as latest from message where message_type !=7 group by chat_row_id) update chat set sort_timestamp=(select latest from timestamps where timestamps.chat_id=chat._id) where hidden=0 and exists (select 1 from timestamps where chat._id=timestamps.chat_id);"

}

minor_fixes() {
  print "Fixing props .." 
  #fixing props
  sql_exec "$output" "delete from props where key='fts_index_start'; 
update props set value=0 where key='fts_ready';"

  print "Removing frequent duplicates .."
  #duplicates
  sql_exec "$output" "delete from frequent where _id in (with freq as (select row_number() over (partition by jid_row_id order by message_count desc) as rn, _id, jid_row_id from frequent) select _id from freq where rn > 1); delete from frequents where _id in (with freqs as (select row_number() over (partition by jid order by message_count desc) as rn, _id, jid from frequents) select _id from freqs where rn > 1);" 2>&"$fd"
  
  print "Fixing message sort_ids .."
  sql_exec "$output" "update message set sort_id=_id;"

}

restore_triggers() {
  print "Restoring triggers .."
  sql_exec "$output" "$(<"$trigger_dump")"
}

clear_tmp_tables_triggers() {
  print "Clearing temp tables and triggers .."
  local map_indexes t
  
  readarray -d "," -t map_indexes < <(sql_scalar "$output" "select name as nm from sqlite_master where name like '%_map77' or name like '%unique_index_77' and type in ('table','index')")
 
  for t in "${map_indexes[@]}"; do
    if [[ "$t" == *"map77"* ]]; then
      sql_exec "$output" "drop table if exists $t;"
    elif [[ "$t" == *"unique_index_77"* ]]; then
      sql_exec "$output" "drop index if exists $t;"
    fi
  done
}

prepare() {
  get_base_db
  setup_output
  clear_bak_triggers
  get_base_tables
  create_unique_indexes
  clear_base_db
  get_base_table_columns
}

merge() {
  local db="$1"
  local table common_main_tables
  readarray -d "," -t common_main_tables < <(printf "%s" "$(get_common_main_tables "$db")")
  
  readarray -d "," -t common_minor_tables < <(printf "%s" "$(get_common_minor_tables "$db")")
  print "Merging main tables .."
  log "Begin Copying tables from '$db'"
  log "Copying main tables"
  for table in "${common_main_tables[@]}"; do
    log "$table"
    insert_rows "$db" "$table"
    create_maps "$db" "$table"
  done
  print "Merging minor tables .."
  log "Copying minor tables"
  for table in "${common_minor_tables[@]}"; do
    log "$table"
    insert_rows "$db" "$table"
  done
  
  log "End Copying tables from '$db'"
}

finally() {
  print "Finalizing .."
  if [[ "$fk" -eq 1 ]]; then
    sql_exec "$output" "pragma foreign_keys=on;"
  fi
  print "Clearing backup_changes table .."
  sql_exec "$output" "delete from backup_changes;"
  
  print "Done. Output is '$output'"
  print "Log is '$log_file'"
}

prepare

for db in "${merge_dbs[@]}"; do
    print "Merging '$db' .."
    merge "$db"
done

# Copying all merged tables to a new clean database (copy) for sorting of messages
print "Sorting Messages"
db="$output"
output="$output_copy"
merge "$db"

# renaming copy database to msgstore.db
output="${output_dir}/msgstore.db"
mv "$output_copy" "$output"

fix_chat
minor_fixes
restore_triggers
clear_tmp_tables_triggers
finally
