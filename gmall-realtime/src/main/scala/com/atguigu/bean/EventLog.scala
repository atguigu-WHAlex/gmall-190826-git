package com.atguigu.bean

//{"area":"beijing","uid":"192","itemid":32,"npgid":24,"evid":"clickItem","os":"ios","pgid":17,"appid":"gmall2019","mid":"mid_492","type":"event","ts":1582338255458}

case class EventLog(mid: String,
               uid: String,
               appid: String,
               area: String,
               os: String,
               `type`: String,
               evid: String,
               pgid: String,
               npgid: String,
               itemid: String,
               var logDate: String,
               var logHour: String,
               var ts: Long)