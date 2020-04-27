## Pet project using pyspark module
<b>PySparkJob.py</b>
clickstream.parquet - parquet file with following structure:
<ul>
<li>date  - день, в который происходят события </li><br>
<li>time - точное время события </li><br>
<li>event 	тип события, может быть или пока или клик по рекламе </li><br>
<li>platform 	платформа, на которой произошло рекламное событие </li><br>
<li>ad_id 	id рекламного объявления </li><br>
<li>client_union_id 	id рекламного клиента </li><br>
<li>campaign_union_id 	id рекламной кампании </li><br>
<li>ad_cost_type 	тип объявления с оплатой за клики (CPC) или за показы (CPM) </li><br>   
<li>ad_cost 	стоимость объявления в рублях, для CPC объявлений - это цена за клик, для CPM - цена за 1000 показов </li><br>
<li>has_video 	есть ли у рекламного объявления видео </li><br>
<li>target_audience_count 	размер аудитории, на которую таргетируется объявление</li>
</ul>
