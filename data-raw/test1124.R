
# 步骤1分组-----
caaspkg::log_qaPair_byDate()
# 2020-08-19
# select *   from t_kf_logGroup_Input
# 所有的数据到input，增加了FGroupID
# FUser,log_date,FGroupId 产生一组数据
#步骤2合并--------
log_qna_combined_byDate()
#使用FGoupId+FIsA作为合并分组依据-------
# combine(FLog) by FGoupId+FIsA------
# 合并后的结果t_kf_logCombined
#增加如下字段**
# 用于配合打配数据*****---------
# res2$FInterId <- 1:ncount_res2 + value_maxId 序号
# res2$Fflag <- ''       是否匹配
# res2$Ftag_by <- ''     关键字
# res2$FIs_valid <- ''   是否有效
# res2$FCategory <-''    业务分类
# end of step2--------

#步骤3针对数据进行打标
#打标的数据
# 输出  t_kf_logTag2
# FUser+log_date+FinterId
# flag是否匹配
# FTag_by 匹配关键词
caaspkg::qnlog_logTagBatch_writeDB(FStartDate ='2020-08-19',FEndDate ='2020-08-19'  )
# sub Func: qnlog_logTag_writeDB -------
#*     sub Function:qnlog_logTag_read--------
#     qnlog_getLog_perUserDate
#     select FUser,author,log_date, FLog as content ,FInterId  from t_kf_logCombined  where FIsA ='FALSE'
#res <- try(tsdo::df_blackList_tagging(data = data,var_text = 'content',dict_contain = dict_contain,dict_equal = dict_eq))
#
# caaspkg::qnlog_logTagBatch_writeDB3(FStartDate ='2020-08-19',FEndDate ='2020-08-19'  )
#
#
# qnlog_getUserDate3(FStartDate = '2020-08-19',FEndDate ='2020-08-19' )
#
#
# mydata <-qnlog_getLog_perUserDate3(log_date ='2020-08-19' )
# View(mydata)
#
# #针对数据进行-----
# mydata_tag3 <-qnlog_logTag_read3(log_date ='2020-08-19' )
# View(mydata_tag3)
#
#
# qnlog_logTag_writeDB3(log_date ='2020-08-19' )


qnlog_logTagBatch_writeDB3(FStartDate ='2020-08-19',FEndDate ='2020-08-19')




#___________________________

mydata3 <-log_qna_combined3(log_date = '2020-08-19')
View(mydata3)


AA <-mydata3$FGroupId[mydata3$FIsA == 'TRUE']
QQ <-mydata3$FGroupId[mydata3$FIsA == 'FALSE']
AA
QQ
AA[!AA %in% QQ]



mydata3[mydata3$FGroupId ==294,]


