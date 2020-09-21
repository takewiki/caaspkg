mydata <- log_qna_combined()



View(mydata)



mydata2 <- qnlog_logTag_read(FUser = '蓝淀',log_date = '2020-08-18')
View(mydata2)

qnlog_getUserDate(FStartDate = '2020-08-18',FEndDate = '2020-08-18')


qnlog_logTagBatch_writeDB(FStartDate = '2020-08-18',FEndDate = '2020-08-18')


caaspkg::log_qaPair_byDate(log_date = '2020-08-04')
caaspkg::log_qna_combined_byDate(log_date = '2020-08-04')
qnlog_logTagBatch_writeDB(FStartDate = '2020-08-03',FEndDate ='2020-08-04' )
log_qna_updatedLogTag()









