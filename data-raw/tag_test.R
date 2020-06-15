library(tsdo)
library(caaspkg)

#data <- qnlog_log_tagging_read()
#View(data)


#qnlog_logTag_writeDB()
qnlog_logTagBatch_writeDB()



























data <- qnlog_getLog_perUserDate()

x <- data[data$content =='在',]
dict1 =qnlog_getBlackList()
dict2 = qnlog_getBlackList_eq()

data <- x

var_text <-'content'
var_flag <-'fflag'
var_tag_by <-'fflag_tag_by'
dict_contain = dict1
dict_equal = dict2

ncount <-nrow(data)
if(ncount >0){
  #针对有数据的情况下进行处理
  for (i in 1:ncount) {


    #针对每一行数据进行处理
    x <-data[i,var_text]
    if(is.na(x)){
      x <- ""
    }
    print(paste0('i',i,x))
    #针对数据进行处理
    #初始化变更
    find <- 0
    flag <- FALSE
    tag_by <-""

    #尝试处理
    try({
      #针对每种情况进行处理,顺序也非常重要
      #1网址  优先识别网址
      if(find ==0 & flag ==FALSE){
        flag <- str_contain_http(x)
        if(flag){
          find <- 1
          tag_by <-"网址"
          print('str_contain_http')
        }
      }
      #1手机号-----
      if(find ==0 & flag == FALSE){
        #判断是否手机号
        flag <- str_contain_phone(x)
        if(flag){
          find <- 1
          tag_by <-"电话号码"
          print('str_contain_phone')
        }
      }
      if(find ==0 & flag == FALSE){
        #判断是否手机号
        flag <- str_contain_num(x,digit = 11)
        if(flag){
          find <- 1
          tag_by <-"11位数字"
          print('str_contain_num')
        }
      }
      #2车架号----
      if(find ==0 & flag ==FALSE){
        flag <- str_contain_vin(x)
        if(flag){
          find <- 1
          tag_by <-"车架号"
          print('str_contain_vin')
        }
      }

      #4卡片
      if(find ==0 & flag ==FALSE){
        flag <- str_contain_card(x)
        if(flag){
          find <- 1
          tag_by <-"[卡片]"
          print('str_contain_card')
        }
      }
      #5图片.
      if(find ==0 & flag ==FALSE){
        flag <- str_contain_picture(x)
        if(flag){
          find <- 1
          tag_by <-"[图片]"
          print('str_contain_picture')
        }
      }
      #6.表情
      if(find ==0 & flag ==FALSE){
        flag <- str_contain_emotion(x)
        if(flag){
          find <- 1
          tag_by <-"[表情]"
          print('str_contain_emotion')
        }
      }
      #7.语音
      if(find ==0 & flag ==FALSE){
        flag <- str_contain_sound(x)
        if(flag){
          find <- 1
          tag_by <-"[语音]"
          print('str_contain_sound')
        }
      }
      # 8.emoji
      if(find ==0 & flag ==FALSE){
        flag <- str_contain_emoji(x)
        if(flag){
          find <- 1
          tag_by <-"[emoji]"
          print('str_contain_emoji')
        }
      }
      #9.?
      if(find ==0 & flag ==FALSE){
        flag <- str_contain_ask(x)
        if(flag){
          find <- 1
          tag_by <-"问号1"
          print('str_contain_ask')
        }
      }
      if(find ==0 & flag ==FALSE){
        flag <- str_contain_ask2(x)
        if(flag){
          find <- 1
          tag_by <-"问号2"
          print('str_contain_ask')
        }
      }
      if(find ==0 & flag ==FALSE){
        flag <- str_contain_ask3(x)
        if(flag){
          find <- 1
          tag_by <-"问号3"
          print('str_contain_ask')
        }
      }
      #10处理空行数据
      if(find ==0 & flag ==FALSE){
        #NA作为空白行进行处理
        if(is.na(x)){
          x <-""
        }
        flag <- str_contain_blank(x)
        if(flag){
          find <- 1
          tag_by <-"空白行"
          print('str_contain_blank')
        }
      }

      #11使用对照表进行处理
      #A处理包含匹配的
      if(find ==0 & flag ==FALSE){
        #针对规则表中的每一个数据处理
        print('step1')
        for (rule in dict_contain) {
          try({
            res <-stringr::str_detect(x,rule)
            if(res){
              print(paste0('j',rule))
              flag <- TRUE
              tag_by<- rule
              find <-1
              break;
            }else{
              next;
            }
          })


        }
      }
      #针对B业务处理，严格相等
      if(find ==0 & flag ==FALSE){
        print('step2')
        #针对规则表中的每一个数据处理
        for (rule in dict_equal) {
          try({

            res <- x == rule
            if(res){
              print(paste0('2j',rule))
              flag <- TRUE
              tag_by<- rule
              find <-1
              break;
            }else{
              next;
            }
          })


        }
      }


      # end for 10

    })



    data[i,var_flag] <- flag
    data[i,var_tag_by] <- tag_by








  }#end for row deal


}

print(data)




