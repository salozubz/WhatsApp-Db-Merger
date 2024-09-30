#declare path for your shebang
#!/bin/bash


old="path_to_old_db"
new="path_to_new_db"

#path_to_new_db should be an empty database which I have provided

function CheckCommonCols() {
  old_cols=$(sqlite3 "$old" "select group_concat('''' || name || '''') from pragma_table_info('$1');")
  c_cols=$(sqlite3 "$new" "select group_concat(name) from pragma_table_info('$1') where name in ($old_cols);")


}



#check common tables
old_tables=$(sqlite3 "$old" "select group_concat('''' || name || '''') from sqlite_master where type='table';")
IFS=$'\n' read -rd '' -a c_tables <<< $(sqlite3 "$new" "select name from sqlite_master where type='table' and name in ($old_tables) and name not like '%fts%' and name not like '%view%' and name not in ('message','messages','message_link','messages_links','message_quoted','messages_quotes','message_vcard','message_vcards','message_vcard_jid','message_vcards_jids','frequents','frequent','sqlite_sequence','message_thumbnail','message_thumbnails','backup_changes');")



for table in "${c_tables[@]}"; do
 CheckCommonCols "$table"
 echo "copying $table"
 sqlite3 "$new" "attach '$old' as old;" "insert into $table ($c_cols) select $c_cols from old.${table};"
done



#convert messages to message
echo "copying messages to message"
sqlite3 "$new" "attach '$old' as old;" "insert into message (_id,chat_row_id,sender_jid_row_id,from_me,text_data,key_id,status,recipient_count,participant_hash,origin,timestamp,received_timestamp, message_type,broadcast,receipt_server_timestamp,starred,sort_id) select om._id,oc._id,ifnull(oj2._id,0),om.key_from_me,om.data,om.key_id,om.status,om.recipient_count,om.participant_hash,om.origin,om.timestamp,om.received_timestamp,om.media_wa_type,0,om.receipt_server_timestamp,om.starred,om._id from old.messages om join old.jid oj on om.key_remote_jid=oj.raw_string join old.chat oc on oj._id=oc.jid_row_id left join old.jid oj2 on oj2.raw_string=om.remote_resource;"

#convert messages_links to message_link

echo "copying message_links to message_link" 
sqlite3 "$new" "attach '$old' as old;" "insert into message_link (_id,chat_row_id,message_row_id,link_index) select oml._id,oc._id,oml.message_row_id,oml.link_index from old.messages_links oml join old.jid oj on oml.key_remote_jid=oj.raw_string join old.chat oc on oj._id=oc.jid_row_id;"


#convert messages_quotes to message_quoted

echo "copying messages_quotes to message_quoted"
sqlite3 "$new" "attach '$old' as old;" "insert into message_quoted (message_row_id,chat_row_id,parent_message_chat_row_id,from_me,sender_jid_row_id,key_id,timestamp, message_type,origin,text_data,payment_transaction_id,lookup_tables) select om._id,oc._id,oc._id,omq.key_from_me,ifnull(oj2._id,0),omq.key_id,omq.timestamp, omq.media_wa_type,omq.origin,omq.data,omq.payment_transaction_id,0 from old.messages om join old.messages_quotes omq on omq._id=om.quoted_row_id join old.jid oj on oj.raw_string=omq.key_remote_jid join old.chat oc on oc.jid_row_id=oj._id left join old.jid oj2 on oj2.raw_string=omq.remote_resource where om.quoted_row_id is not null and om.quoted_row_id !=0 order by om._id;"



#message_vcards to message_vcard
echo "copying message_vcards to message_vcard"
sqlite3 "$new" "attach '$old' as old;" "insert into message_vcard (_id,message_row_id,vcard) select _id,message_row_id,vcard from old.messages_vcards;"

#message_vcards_jids to message_vcard_jid
echo "copying message_vcards_jids to message_vcard_jid"
sqlite3 "$new" "attach '$old' as old;" "insert into message_vcard_jid (_id,vcard_jid_row_id,vcard_row_id,message_row_id) select ovcj._id,oj._id,ovcj.vcard_row_id,ovcj.message_row_id from old.messages_vcards_jids ovcj join old.jid oj on oj.raw_string=ovcj.vcard_jid;"


#frequents to frequent
echo "copying frequents to frequent"
sqlite3 "$new" "attach '$old' as old;" "insert into frequent (_id,jid_row_id,type,message_count) select ofs._id,oj._id,ofs.type,ofs.message_count from old.frequents ofs join old.jid oj on ofs.jid=oj.raw_string;"


#message_thumbnails to message_thumbnail
echo "copying message_thumbnails to message_thumbnail"
sqlite3 "$new" "attach '$old' as old;" "insert into message_thumbnail (message_row_id,thumbnail) select om._id,ot.thumbnail from old.message_thumbnails ot join old.messages om on om.key_id=ot.key_id order by om._id;"

#fixes quotes 
sqlite3 "$new" "update message set lookup_tables=2 where _id in (select message_row_id from message_quoted);"


sqlite3 "$new" "delete from backup_changes;"
echo "complete"