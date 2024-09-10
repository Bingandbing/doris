// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_match_and_join") {

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql """ drop table if exists table_30_undef_partitions2_keys3_properties4_distributed_by55 """
    sql """ 
    create table table_30_undef_partitions2_keys3_properties4_distributed_by55 (
        pk int,
        col_int_undef_signed_index_inverted int  null  ,
        col_int_undef_signed int  null  ,
        col_int_undef_signed_not_null int  not null  ,
        col_int_undef_signed_not_null_index_inverted int  not null  ,
        col_date_undef_signed date  null  ,
        col_date_undef_signed_index_inverted date  null  ,
        col_date_undef_signed_not_null date  not null  ,
        col_date_undef_signed_not_null_index_inverted date  not null  ,
        col_varchar_1024__undef_signed varchar(1024)  null  ,
        col_varchar_1024__undef_signed_index_inverted varchar(1024)  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_true varchar(1024)  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_false varchar(1024)  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_true varchar(1024)  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_false varchar(1024)  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained varchar(1024)  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained varchar(1024)  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_false varchar(1024)  null  ,
        col_varchar_1024__undef_signed_not_null varchar(1024)  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted varchar(1024)  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_true_parser_english varchar(1024)  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_parser_english_support_phrase_false varchar(1024)  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_true varchar(1024)  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_false varchar(1024)  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained varchar(1024)  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained varchar(1024)  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_false_parser_chinese varchar(1024)  not null  ,
        INDEX col_int_undef_signed_index_inverted_idx (`col_int_undef_signed_index_inverted`) USING INVERTED,
        INDEX col_int_undef_signed_not_null_index_inverted_idx (`col_int_undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX col_date_undef_signed_index_inverted_idx (`col_date_undef_signed_index_inverted`) USING INVERTED,
        INDEX col_date_undef_signed_not_null_index_inverted_idx (`col_date_undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX col_varchar_1024__undef_signed_index_inverted_idx (`col_varchar_1024__undef_signed_index_inverted`) USING INVERTED,
        INDEX col_varchar_1024__undef_signed_index_inv_0_idx (`col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_true`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true"),
        INDEX col_varchar_1024__undef_signed_index_inv_1_idx (`col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_false`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "false"),
        INDEX col_varchar_1024__undef_signed_index_inv_2_idx (`col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_true`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
        INDEX col_varchar_1024__undef_signed_index_inv_3_idx (`col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_false`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "false"),
        INDEX col_varchar_1024__undef_signed_index_inv_4_idx (`col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "true", "parser_mode" = "coarse_grained"),
        INDEX col_varchar_1024__undef_signed_index_inv_5_idx (`col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "true", "parser_mode" = "fine_grained"),
        INDEX col_varchar_1024__undef_signed_index_inv_6_idx (`col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_false`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "false"),
        INDEX col_varchar_1024__undef_signed_not_null_index_inverted_idx (`col_varchar_1024__undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX col_varchar_1024__undef_signed_not_null__7_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_true_parser_english`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true"),
        INDEX col_varchar_1024__undef_signed_not_null__8_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_parser_english_support_phrase_false`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "false"),
        INDEX col_varchar_1024__undef_signed_not_null__9_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_true`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
        INDEX col_varchar_1024__undef_signed_not_null__10_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_false`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "false"),
        INDEX col_varchar_1024__undef_signed_not_null__11_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "true", "parser_mode" = "coarse_grained"),
        INDEX col_varchar_1024__undef_signed_not_null__12_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "true", "parser_mode" = "fine_grained"),
        INDEX col_varchar_1024__undef_signed_not_null__13_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_false_parser_chinese`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "false")
        ) engine=olap
        DUPLICATE KEY(pk, col_int_undef_signed_index_inverted)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """ 

    sql """
        insert into table_30_undef_partitions2_keys3_properties4_distributed_by55(pk,col_int_undef_signed,col_int_undef_signed_index_inverted,col_int_undef_signed_not_null,col_int_undef_signed_not_null_index_inverted,col_date_undef_signed,col_date_undef_signed_index_inverted,col_date_undef_signed_not_null,col_date_undef_signed_not_null_index_inverted,col_varchar_1024__undef_signed,col_varchar_1024__undef_signed_index_inverted,col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_true,col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_false,col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_true,col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_false,col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained,col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained,col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_false,col_varchar_1024__undef_signed_not_null,col_varchar_1024__undef_signed_not_null_index_inverted,col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_true_parser_english,col_varchar_1024__undef_signed_not_null_index_inverted_parser_english_support_phrase_false,col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_true,col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_false,col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained,col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained,col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_false_parser_chinese) values (0,0,-1603201726,2,1,'2023-12-14','2024-02-18','2023-12-17','2011-04-03','and I''ll didn''t would her him back see','放心','','hey','see','--','注册','?','谈好','they','型号呢里交流你是谁柏霖焦作市中旬郑娜出团','?','g','肥姐也冲击随其自然每次鹏飞叫做亲戚华阳','we because who look about you''re as','h','金海','your so up at right got because who'),(1,2,null,1073463547,-1,'2023-12-11',null,'2024-02-18','2023-12-19','未税',null,'?','have don''t get now but','核对过','will','as','could','say I''m','?','?','签订','节点快乐今天儿童经销总代理出面','v','how','get he''s can yeah no her yes oh he''s would','青云不知道阿奎那干嘛维泽','r'),(2,1,1777219286,-2,2011685283,null,'2026-01-18','2025-06-18','2023-12-18','t','','使人','been',null,'?','','分公司平志伟长光','all oh are they his','调价东风路','c','先锋','宝龙','','彭巧语稳定','that''s out it about','good','want okay now he go do you''re something'),(3,1,6,7,0,'2025-06-18','2023-12-13','2024-02-18','2025-02-18','','?','分享合创人参金鳞','直管问起','','','走在网络失误','ok',null,'yeah back how yeah come he i hey were do','序列号','不懂','?','go','丰富','are don''t','then','泥石谁'),(4,-2,3,-1564396755,1197391189,'2026-02-18','2024-02-18','2023-12-16','2023-12-09','now','k','come you''re had I''ll in are look he''s on to','go',null,'ok you was','雅虎相关天天自私休息许昌有种效果','南天',null,'有些时候','?','could','his','--','of','we he can on and all','--','had'),(5,null,8,-2,0,null,'2023-12-14','2023-12-13','2024-02-18','','say','胡华威','','g','out yeah her didn''t for what like can''t not','about','r','--','?','something','?','--','l','再来全程若虚','一颗','','oh know was how when just not'),(6,-1,null,-2,-2,'2025-06-18','2023-12-15','2024-02-18','2024-01-17','x','','have as are know of a','b','time','at','her here know you your were as mean','a','--','know','in','家里应该山东能不鞥','我刚回来呀','--','--','?','原装双千兆莱克特上面妹妹飞机多久笔记本治疗那捕鲸船','?'),(7,null,null,-1723016302,-2,'2025-02-18','2024-01-19','2023-12-16','2025-02-17','--','晓得','on he''s with','--','电厂','小浪底接触','look','有点','','?','f','怎么样份款','want there did yes','老公你这个满意老刘勇气四天离谱有数广大不敢','back been at like','say not really','of all are could the know would','are'),(8,38379557,null,-1,-1,'2024-01-31','2027-01-09','2025-02-18','2024-01-09','--','分钟','will','惭愧李静加入我刚来呀颜色','look i got we really ok','谈谈','who','something',null,'','didn''t if','','j','证明金信会刊单挑刘金军港','--','--','going it',''),(9,968361924,0,-2,-2,null,'2023-12-16','2023-12-09','2023-12-19','got who tell in didn''t','been I''ll i if don''t it don''t then had','--','?','听着逍遥预计和兴涨价暴风影音就在埋在拼搏','原厂海信大品牌腾龙瑞丽盛源几天就走许愿树详谈','didn''t','don''t look me I''ll','','优惠最起码热卖不已本公司嘉通机会底下','mean','t','not not of come don''t go i it','up what come','going can say the time look of when','so','为什么',''),(10,-1,-1,752321536,-819779964,'2025-06-18','2024-01-31','2023-12-20','2023-12-12','--','?','挑战散落的反应都是准备都还多久大公司','','金蝶','',null,'you''re','热线在那普惠质保金烟火城市','?','one','','get it','b','负责','with','he i have about hey or you','中科部分一鸣国标'),(11,-1,3,-2,-1,'2023-12-13',null,'2025-02-17','2023-12-14','讨论组','?','they','鼠标','think he your want know','I''ll','it see in','--','see','图美创世纪','还跟他快运小曹别人','样机','有机会算错','go have had as all do I''ll','f','got all can didn''t something back had no','so','time'),(12,null,-1,5,0,'2023-12-15','2023-12-13','2023-12-20','2023-12-19',null,'?','询问无论汉威叁万领导地方亮度','注明','at just look from been at time','been','know your you''re','with now when in know been as I''ll','my he time with come','着呢很多','--','大学老师照片快运','?','could it if at tell was oh and yeah','--','all','不会被班车学校交叉照片治疗严格分数加载浪漫','?'),(13,-1,null,210811703,9,'2026-02-18','2002-12-15','2025-06-18','2026-02-18','不愿意','','know what','甘心是不是办事处数字见你难说发吧发吧晚上给把太原','but','贸易通','it really mean all how could when with all',null,'know something','now were all can''t can my tell oh they','didn''t up','真理','that''s know yeah i','d','as he they would been something his I''ll','张总曙光主机油漆正常','','was'),(14,-1,1291248968,1,-1,'2024-01-31','2024-01-19','2023-12-17','2024-02-18',null,'','when not not because back there so',null,'as ok but about up at see could come',null,'美女及其胡提高性能女孩健民没沈晓海不含','?','can','--','what say not she about is','齐村据对中病毒你问火球绿城只能唯一的','','便宜','--','','--','--'),(15,-1,-1,-1,-186244435,'2024-02-18','2024-02-18','2023-12-11','2027-01-09','so i come','right','because','做好','who','调价付费生活炒货谁家有货看着要不然心事报价格大话',null,'市场','','about do go know all','弄个','汇宝单单自己国栋底下刻录新款一周心情','停留批单','重新发一天第三方管理一千多绝对','me this she you''re were on','--','I''m','we to is he was not she'),(16,6,-2,-623692175,1438177278,'2025-06-18','2025-02-17','2024-01-09','2024-01-09','?','x','周星驰四川力天','','','老婆明白泰杰斯操作合计难说地市做方案','o','mean','--','外面不完利万','?','?','x','k','吃过饭仪器','as is like','--','--'),(17,1,-2,-2,-2,null,'2023-12-10','2023-12-16','2024-01-17',null,'g','did I''m her would could','回忆订票周星驰礼拜天代表王佩丽进一步房地产','think','can','look or want and no yeah','','','长信请你','组哟','同方','--','g','?','加密','外运','提示到来派人郑州还有要不然高亮屏你呢岂能被人'),(18,6,6,236306652,5,'2024-01-17','2023-12-15','2026-01-18','2023-12-11','安全正好标底圆圆跨区分钟不多','你用手机吧','it''s him back or it''s just','?','只有南阳路回家诊断他说你还没有回来啊北京报价格','了吗舍得良子停机都想共同三润','a','about','hey','he''s out to because ok mean','say','','his','命名层次还不错附上作风','光电清理着那个家不止它是热销中','think all and','核对轩明令你','about'),(19,-1,null,-1,-1,null,'2023-12-11','2023-12-11','2023-12-17','?',null,'what going from','莱克特','--','惊喜礼物刘海开头所谓开封吃肉我刚回来呀','子卡','y','who I''m now all to like got why','春江寸草心合作过半天班安小瑞达证明一次','--','out','有人你好呀重要机架短暂天地','yeah but on be did or there when','she','get here it''s see could like what would','','yeah her your'),(20,1256894923,-1,-1772431512,-1,'2023-12-12',null,'2027-01-09','2023-12-13','还得东风系列则会有谈好主演少秋经营第二','一夜开除做完疲劳单挑','or they','?','','no',null,'导入液晶移动关键','?','主办','心情常用生意说说提示','go one been yeah as','','会员卡','not get','伤心双飞燕安装','it''s','售后'),(21,-2,2,-1,-1,'2023-12-14','2023-12-11','2012-01-08','2025-02-17',null,'but','--','','would','教练史丽威达机柜唯一对了五万条快乐在乎志彬','a','电子卡将','a','','going','','拟在建爱找到严重商人大量晓燕写出来重要性营销聪明','独立','背景','l','d','back that were because will some going it did on'),(22,132295595,null,-1,-1,'2023-12-12','2025-06-18','2026-01-18','2026-02-18','相约总代也要想起来接收','?','?','?',null,'方式','?','认可','工业大学','?','one but','?','have','什么弯弯的月亮一套节省动物上有盘点始终','盛源分辨率另外图站','我给你打电话中病毒珍贵苦楚','小曹文宁正式版刀片柯艳琦','--'),(23,null,-1,1444541305,2,'2023-12-14','2023-12-14','2024-01-17','2023-12-17','would here from just going','out','about now back know it''s','丁丁圣荣松伟我还港湾金海','well it well of here is really','--','已经创世纪','?','','your','拿住网页委托收款海星报名说不出电厂通过短句','弟子快点','ok on this the see about at you you','no something don''t because see will why at she yeah','兴致订货治好出团预算赵总冲动','out why he''s her is that''s well','mean','?'),(24,-1482687319,5,-1,-1,null,'2023-12-10','2023-12-16','2023-12-20','','多少钱一群立刻你问若虚顺风晚上',null,'业茂','--','?','','使用','d','will in well','拿住','are','做好','say not it will for come no but so','--','位置综合学员能不鞥很好用文宁','no','not'),(25,null,4,8,5,'2023-12-17','2024-02-18','2024-01-31','2023-12-19','?','one they okay have when you''re','共同应该股份有限公司处理一下','--','离开','动作','would','客运量特此证明地区真理初次肆万小楼听雨北京','成交这块工商局','--','but','think','地区','because','--','?','网易有空吗晚上给把一颗真正以以上的授狗那里指点','图像'),(26,3,-792244912,-1,-2,'2023-12-20','2027-01-16','2027-01-09','2023-12-12','惊喜总代理商部队款且','?','x','王总签不签都浪潮年前分享应用独立雪松路黄委会冲动','yeah','简单嘉运达那么饲料核对过蠢材最新版本处理一下百川售后','at','','科贸被授予谈下来冀海潮交流群','here been yes because','先打款不忙','','then right it all not','?','--','why come going on out','k','资格'),(27,-1,-1,-2,1216258389,'2023-12-09','2024-01-19','2024-01-09','2023-12-20','--','?','喝酒','几下','?','know','?','洗手间','?','小键盘','will go did','身份证造成怎么着你吃法三业','o','--','k','--','号码学员星星珊瑚版','传真'),(28,7,-1844462362,6,-2,null,'2024-01-09','2023-12-14','2023-12-20','不然佩利飞机','对吧','come get why how to is what','he','do','发票浪漫没人','你问','--','?','说句','?','u','are','y','I''ll','oh in he''s i yeah no','what it''s','大学路商联'),(29,845502225,1862705239,-2,-1,'2023-12-10','2024-01-31','2024-01-17','2023-12-18','',null,null,'about ok about come for here','?','','--','口语铭岳刚出门热备不够','I''ll','没有直销免费想要周鹏留个鑫海源签字高亮屏漫长','办好未税图站','--','联恒你们已经上班了柏霖方向楼下群管理员保证良子能用吗神龙','just','','--','?','some');
    """

    sql """ drop table if exists table_300_undef_partitions2_keys3_properties4_distributed_by5 """
    sql """
        create table table_300_undef_partitions2_keys3_properties4_distributed_by5 (
        col_int_undef_signed int/*agg_type_placeholder*/  null  ,
        col_int_undef_signed_index_inverted int/*agg_type_placeholder*/  null  ,
        col_int_undef_signed_not_null int/*agg_type_placeholder*/  not null  ,
        col_int_undef_signed_not_null_index_inverted int/*agg_type_placeholder*/  not null  ,
        col_date_undef_signed date/*agg_type_placeholder*/  null  ,
        col_date_undef_signed_index_inverted date/*agg_type_placeholder*/  null  ,
        col_date_undef_signed_not_null date/*agg_type_placeholder*/  not null  ,
        col_date_undef_signed_not_null_index_inverted date/*agg_type_placeholder*/  not null  ,
        col_varchar_1024__undef_signed varchar(1024)/*agg_type_placeholder*/  null  ,
        col_varchar_1024__undef_signed_index_inverted varchar(1024)/*agg_type_placeholder*/  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_true varchar(1024)/*agg_type_placeholder*/  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_false varchar(1024)/*agg_type_placeholder*/  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_true varchar(1024)/*agg_type_placeholder*/  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_false varchar(1024)/*agg_type_placeholder*/  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained varchar(1024)/*agg_type_placeholder*/  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained varchar(1024)/*agg_type_placeholder*/  null  ,
        col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_false varchar(1024)/*agg_type_placeholder*/  null  ,
        col_varchar_1024__undef_signed_not_null varchar(1024)/*agg_type_placeholder*/  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted varchar(1024)/*agg_type_placeholder*/  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_true_parser_english varchar(1024)/*agg_type_placeholder*/  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_parser_english_support_phrase_false varchar(1024)/*agg_type_placeholder*/  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_true varchar(1024)/*agg_type_placeholder*/  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_false varchar(1024)/*agg_type_placeholder*/  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained varchar(1024)/*agg_type_placeholder*/  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained varchar(1024)/*agg_type_placeholder*/  not null  ,
        col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_false_parser_chinese varchar(1024)/*agg_type_placeholder*/  not null  ,
        pk int/*agg_type_placeholder*/,
        INDEX col_int_undef_signed_index_inverted_idx (`col_int_undef_signed_index_inverted`) USING INVERTED,
        INDEX col_int_undef_signed_not_null_index_inverted_idx (`col_int_undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX col_date_undef_signed_index_inverted_idx (`col_date_undef_signed_index_inverted`) USING INVERTED,
        INDEX col_date_undef_signed_not_null_index_inverted_idx (`col_date_undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX col_varchar_1024__undef_signed_index_inverted_idx (`col_varchar_1024__undef_signed_index_inverted`) USING INVERTED,
        INDEX col_varchar_1024__undef_signed_index_inv_0_idx (`col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_true`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true"),
        INDEX col_varchar_1024__undef_signed_index_inv_1_idx (`col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_false`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "false"),
        INDEX col_varchar_1024__undef_signed_index_inv_2_idx (`col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_true`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
        INDEX col_varchar_1024__undef_signed_index_inv_3_idx (`col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_false`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "false"),
        INDEX col_varchar_1024__undef_signed_index_inv_4_idx (`col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "true", "parser_mode" = "coarse_grained"),
        INDEX col_varchar_1024__undef_signed_index_inv_5_idx (`col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "true", "parser_mode" = "fine_grained"),
        INDEX col_varchar_1024__undef_signed_index_inv_6_idx (`col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_false`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "false"),
        INDEX col_varchar_1024__undef_signed_not_null_index_inverted_idx (`col_varchar_1024__undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX col_varchar_1024__undef_signed_not_null__7_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_true_parser_english`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true"),
        INDEX col_varchar_1024__undef_signed_not_null__8_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_parser_english_support_phrase_false`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "false"),
        INDEX col_varchar_1024__undef_signed_not_null__9_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_true`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
        INDEX col_varchar_1024__undef_signed_not_null__10_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_false`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "false"),
        INDEX col_varchar_1024__undef_signed_not_null__11_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "true", "parser_mode" = "coarse_grained"),
        INDEX col_varchar_1024__undef_signed_not_null__12_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "true", "parser_mode" = "fine_grained"),
        INDEX col_varchar_1024__undef_signed_not_null__13_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_false_parser_chinese`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "false")
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
    insert into table_300_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed_index_inverted,col_int_undef_signed_not_null,col_int_undef_signed_not_null_index_inverted,col_date_undef_signed,col_date_undef_signed_index_inverted,col_date_undef_signed_not_null,col_date_undef_signed_not_null_index_inverted,col_varchar_1024__undef_signed,col_varchar_1024__undef_signed_index_inverted,col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_true,col_varchar_1024__undef_signed_index_inverted_parser_english_support_phrase_false,col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_true,col_varchar_1024__undef_signed_index_inverted_parser_unicode_support_phrase_false,col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained,col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained,col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_false,col_varchar_1024__undef_signed_not_null,col_varchar_1024__undef_signed_not_null_index_inverted,col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_true_parser_english,col_varchar_1024__undef_signed_not_null_index_inverted_parser_english_support_phrase_false,col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_true,col_varchar_1024__undef_signed_not_null_index_inverted_parser_unicode_support_phrase_false,col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained,col_varchar_1024__undef_signed_not_null_index_inverted_parser_chinese_support_phrase_true_parser_mode_fine_grained,col_varchar_1024__undef_signed_not_null_index_inverted_support_phrase_false_parser_chinese) values (0,-2,-1901730183,5,-1,'2023-12-14',null,'2023-12-16','2023-12-20','肯定','华康','--','from but when okay would when','','弹出','from my','下月很难说奇偶上班不止单位接受','加密狗铁路董事长重命名策略一点河南压力为你','提示硕博伟泽聊聊冰河心理学对吧','中小金汇通','then can','','will there don''t had did want been some','f','过年','her','--'),(1,1382192944,-1,-234802337,-1,'2025-02-18','2025-02-17','2023-12-12','2027-01-16','国务卿','','look back i in you me say look go','孙瑞霞很想成本还是记录那位','well do he say can it''s oh don''t','--','--','got','think','there','合计回公司遮盖很大置业','','成绩','from','what','原因的','','是的交单记得拆机有点不好漫长司军几关系'),(2,-1,-2,2,-2,'2004-04-02','2023-12-19','2023-12-15','2023-12-12','up','--','汇众孤独办款名家生后打算武汉瀚海答案','--','f','could',null,'差价图形工作站中卢有伴随熟悉大量中创热备软件产生休息','','是你呀有车吗','不过图腾水哦这话大家冀海潮','good say like','my he some yes come so','--','or','?','a come a','see'),(3,-2,-2,-2017523107,-2,'2023-12-17','2027-01-16','2024-01-31','2023-12-17','你家报表表情','不走除非飞信许昌后勤教练新买这一生不下令你','你杀时间回来',null,'time there he well it with me','排列','表情','招工','right','请他','I''m mean be from','','?','完美','here my','b','关于','c'),(4,1065841778,-2,-1,-1,'2024-02-18',null,'2025-06-18','2023-12-20','左右','','?','面子故事离开邮箱合适','ok why here time in that','等级','i','--',null,'be look out of had we to what be don''t','no can hey to get','or as you''re about','go all her some well','只有塔式海花见你名称也冲击幸福跟进中核对过只有','see could i','?','?','海星动态刻意海星刘畅上不了学习'),(5,1458245006,null,-1,-2,'2026-01-18','2023-12-13','2024-01-31','2024-01-08','?','v','when to could','--','发布神秘一小时试过备用金','g','用管','v','这是','is','g','','then','--','','','王哥','p'),(6,3,-1,-239832171,-1,'2024-01-08','2024-01-17','2023-12-12','2026-02-18','is well if all time have you''re','n','?','have',null,'键盘鼠标',null,'第二上门十分新远方表达商丘','the going were at mean','there like been on and yes what or','--','--','go','it''s','花月夜','什么时间','行业屏保长得','我在政治哦同志们参加'),(7,-2,-2,-1,1754315155,'2024-02-18','2023-12-19','2025-02-18','2023-12-14','','--','','显卡宇瑞鑫海源关系','or don''t no ok is',null,'科技大厦经常实现航务新增而是阿奎那纸业自动','x','寝室','then','用户','i how how','一两智博在不在数据公寓电粉','礼拜天','well you here so been what okay see something','--','河南总经销','?'),(8,112676751,-127780972,-1937697475,5,'2024-01-17','2023-12-11','2023-12-16','2024-01-09','三阳','you''re','who','残品作风折旧到底怎么办正规承认又给','','have','r','r','j','if is in','空间冗余','his','','浇水过节费先付新增综合杨学说不','你不认识华阳跟着预计长光理光许昌想象','e','','千金屏保九九马马虎虎怡海'),(9,3,-957042490,3,-1,'2023-12-09','2023-12-20','2023-12-16','2023-12-09',null,'每一小时',null,'was',null,'','广州市','me or you''re look right got when you''re about','it''s okay this up how are for','t','was she don''t at i know can when because','go','how here ok okay','got her he it''s could can''t back had about have','she','?','he''s him they well I''m yeah',''),(10,-200104483,-1,4,-2,'2023-12-16','2025-06-18','2024-02-18','2024-02-18','see','j','今晚','how','态度','he','will','him','不多','had about did','眼睁睁','--','?','打电话','到来','利盟','w','歘俩'),(11,-1,null,-2,-2,null,'2023-12-10','2025-02-18','2024-01-08',null,'生物','你吃法','my','这几太难','状态第二次郑娜备份文件三个业务下班','零捌','time I''ll about on they because don''t good why will','就算格飞五洲一块小妹特性吃过饭叁仟肯定写上去','--','大雪服务站光纤跃民独显从你那里石龙汇宝真是','','on get you''re will','him','why','分开信阳','填写','him of yeah my get they can'),(12,7,8,1567572056,-2,'2023-12-13','2025-02-18','2025-06-18','2023-12-11','命名操心你怎么去的保密中档说吧淀粉英联生命中这几天','some','则会有','充满',null,'领出','i','would oh me are','was','字库文件','?','老公好早一定','见面蓝牙','?','不如孙海洋一辈子蓝牙晓燕','--','it didn''t or hey some think do like here','自私空间'),(13,null,-2079628735,-2,1,'2023-12-11','2023-12-15','2023-12-18','2024-01-09','','--','?','停产','?','农村一起',null,'','一种下了期待刘总圣诞节金汇通千万','见到李东贸易通','?','?','have get did the i to then','for','--','?','线上输入不再梦想朱晓明','who with don''t for that ok him can one'),(14,7,-1104140591,-2,-1,'2023-12-16','2024-01-08','2025-02-17','2023-12-10',null,null,'oh ok','a','?','也许','?','could you','','对你以上有人多家','请他信海维修总之张海燕孤独过去列表奖金','say','or','it''s','o','so that''s she she yeah the right if','?','?'),(15,-2,-1,-2,-1,'2023-12-09','2025-02-17','2024-01-17','2023-12-13','今晚','up ok can but i do one','','will','s',null,'v','b','王枫','come','广发','for I''m now can hey when because time come','?','were','go some ok something of','富通你好删过刚刚','when yeah think how I''ll now','--'),(16,1,null,1025070939,-1065499727,'2024-02-18','2025-02-17','2023-12-19','2024-01-31','','富通怡海赶快瑞星左右河南经销','not','姓江','was','何时','v','me can''t now he ok is been from hey we','o','--','--','we','s','b','that for here at you''re can''t right hey yes','come hey on here yes mean going it''s about or','j','中铁'),(17,1406750472,null,-2,5,'2023-12-16','2024-01-09','2024-01-08','2024-02-18','?',null,'the been is but then can''t yes him this really','really okay say tell how','但愿',null,'--','we right that was I''m','?','','','something','五万多条','--','','','在家里主页绝对北环备份文件','几个工业大学带你正道直接费用科峰不需要鑫海源接口'),(18,6,-1,1073335855,6,'2023-12-18','2023-12-19','2025-02-17','2023-12-10','hey','ok','have was my like','万一证明雅虎','his at didn''t it','分销商中层经理网通等于','维泽特配机器备份文件招工河南经销新普怡海','不可能奖励空间冗余加密狗分钟玉清','?','can''t not','c','空间冗余','--','u','--','have at now was yes have out','would then now is','y'),(19,null,-2,-2,-1,'2024-02-18','2025-06-18','2027-01-16','2024-01-31','could good','up i I''ll','mean','--','--','记得一岁无聊也得空间冗余打印机','英特','客户',null,'','got','明月这个扩展名城市科贸','世安误区独显安装愿意联盟比爱白贰台','?','简介','come a that''s or','?','go'),(20,-2,null,-2,634169930,'2023-12-16','2024-01-17','2024-01-17','2023-12-16','be','?','','--','had','天孝','','then ok to that think this all you like will','细节新闻有钱老公张小莉这点','河南总经销个人自己','有些时候在呀自己的刷卡','j','','I''ll all or','that no why your then okay some her I''ll i','','have','帐户宠物关机也是'),(21,-1,null,-1,0,'2025-02-17','2024-01-31','2024-01-08','2024-01-31','等于合作单位看待解难答案三种','','折扣不行立博我那个叫东西上半年沃尔普会员卡','那款武警总队今晚华北区输得起不曾很想熊猫','?',null,'but out ok how','out',null,'it''s','','三阳开泰','on and he''s and one know know yes good or','帮帮','伟泽这么双子安阳咨询服务时间磁盘柜高密度','速度钢琴曲金牌总代理顺河路彩霞金立下一部分广告下了黄河科学','p','why'),(22,-1,1,-2,-2,'2027-01-16','2024-01-09','2023-12-15','2023-12-15','工资','are really I''m','when of was in well a get think well','are','--','know','?','well say about been going why would yeah out go','?','r','did he''s be something out can''t some him be','','痛苦私人水货绿城打击不起网站关键','不说需要帮忙做到经销商历来三润十来年记得','your in no have got','or look','磁盘你们已经上班了丹尼斯李金才泡茶等等赵洪应一共','进入了'),(23,-1,-2,-2,-2,'2023-12-09','2024-02-18','2023-12-16','2023-12-16','本人','屠龙记','at','瑞昌','?','r','he had are going he''s because from would',null,'we','p','你不中小','all he good go would go my up a what','中天','didn''t','why','w','联盟新闻重新化验好处职务其中欧颜色',''),(24,-2,null,-2117558545,720042588,'2027-01-09','2014-06-24','2023-12-19','2025-02-18','?','l','been you i','广大两千量大优惠索尼','c','come go know when','新买','忘不了大部分有误国有企业','this','加为好友','I''m','your were just as of look good a your or','侠诺灵生不敢电视海川回你在家里海花本科市场上','for didn''t','最重要哪呢个五万多条难过姑娘','for about the I''m yeah go hey','我刚回来呀','--'),(25,9,-2,-1,-1562783499,'2024-01-09','2025-06-18','2025-02-17','2024-01-08','珍惜不再','--','下个','?','电话','some see go from don''t that''s be really','杀毒你来找我吧日常也有过政府','鼠标','you''re here back about she we some don''t they','--','中的','--','on say is on what','卖友公路说吧答案','这款','结果','基地连续公司近台南不愿意','q'),(26,null,null,566690289,-1,'2024-01-17','2023-12-19','2023-12-13','2024-02-18','about','隆康王子','就有','n','中层经理','购买','虚妄主奴婢最近忙什么呢找你见到新区外面','be is','','北环私人郑大沧田第一次请他肛门锐成文件夹浪潮','下来','?','z','生活屏保派送','?','--','万方雷林授权发生过焕然去不了生意还以为直供','he one go'),(27,8,-1329519821,5,-1,'2023-12-19','2023-12-14','2023-12-20','2025-06-18','some they want I''ll they yeah going','花园','原因','?','中午随风很好用珊瑚版比人很多输得起售后王燕纯度',null,'i','because i look what','--','his','why','--','that''s','the it for some are don''t here','oh back say they are from','新密韩鸽飞空分电脑一部一万有机会开发强暴找你','你休息吸纳中脱机正式版视讯重要性客户群以上','it''s'),(28,4,7,738661869,1,'2024-02-18','2024-01-19','2025-02-18','2023-12-15','?',null,'都想','王星银河外运保留令你年后使人瑞星','?','hey up','we say','科技市场','刘雪','s','--','something','tell','办事处进去祝福','get with hey that''s back','玉冰','老刘批发价忙碌小型机','三石'),(29,0,143593368,-2,335460636,'2026-02-18','2025-02-18','2025-06-18','2026-01-18',null,'暂时控制卡','?','知道','b','占到热线我给你打吧企业方法首创那你只能员工','','安信情歌内存王晶威达他娘的语文说好','离开逝去自己小苏网卡新一代武汉方法色戒','找不到','time','小楼听雨宝贵不但是忙碌飞扬农科阿奎那一套虚妄','废话王总签不签都各位收集','既然','宫颈糜烂','惊喜金牌','this','');
     """

    qt_sql """
        select count() from table_30_undef_partitions2_keys3_properties4_distributed_by55 t1, table_300_undef_partitions2_keys3_properties4_distributed_by5 t2 where t1. col_date_undef_signed_index_inverted is not null or (t1. col_varchar_1024__undef_signed_index_inverted_parser_chinese_support_phrase_true_parser_mode_coarse_grained MATCH "儿童") and not (t2. col_int_undef_signed is null) and not (t1. col_int_undef_signed_not_null_index_inverted is null or t1. col_int_undef_signed_not_null_index_inverted is not null);
    """

}