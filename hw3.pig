/*-----------------------------------------------------------
-------------------------------------------------------------
Author: Khushboo Tekchandani
Course: CSE587
UBIT: ktekchan
HW3 
-------------------------------------------------------------
-------------------------------------------------------------
*/


/*Store data of the stock file in stock_data. Prepends filename.*/

stock_store = LOAD 'hdfs:///pigdata' USING PigStorage(',','-tagFile');

stock_strip_hdr = FILTER stock_store BY $1 neq 'Date';
stock_data = FOREACH stock_strip_hdr GENERATE $0, (chararray)$1, (float)$7;

stock_temp = FOREACH stock_data GENERATE $0, FLATTEN(STRSPLIT($1,'-')) , $2;

/*Get the Adjacent close price for the first and last day of each month*/
stock_yymm = GROUP stock_temp BY ($0,$1,$2);
stock_first_last = FOREACH stock_yymm {
	sorted_a = ORDER stock_temp BY $3 ASC;
	sorted_d = ORDER stock_temp BY $3 DESC;
	lim_a = LIMIT sorted_a 1;
	lim_d =	LIMIT sorted_d 1;
	GENERATE FLATTEN(lim_a),FLATTEN(lim_d);
}

/*Get Monthly Rate of Return, average Monthly Rate of Return and Number of Months*/
stock_MRR = FOREACH stock_first_last GENERATE $0, ((float)($4-$9)/(float)$4) ;
stock_group = GROUP stock_MRR BY $0;
stock_avg = FOREACH stock_group GENERATE $0, (float)AVG(stock_MRR.$1), COUNT_STAR(stock_MRR.$1);

/*Calculate other necessary values*/
stock_join = JOIN stock_group BY $0, stock_avg BY $0;
stock_join1 = FOREACH stock_join GENERATE FLATTEN($1), $3, $4;

/*Stock Volatility*/
stock_vol1 = FOREACH stock_join1 GENERATE $0, ((float)($1-$2)*($1-$2)), $3;

stock_vol2 = GROUP stock_vol1 BY ($0,$2);
stock_vol3 = FOREACH stock_vol2 GENERATE FLATTEN($0), SUM(stock_vol1.$1);

stock_vol = FOREACH stock_vol3 GENERATE $0, SQRT((float)(1/(float)($1-1))*$2);
stock_vol_nonull = FILTER stock_vol BY $1!=0.0;

/*Sort the Volatility in ascending order and get Top 10 Min*/
stock_sort1 = GROUP stock_vol_nonull ALL;
stock_sort_min = FOREACH stock_sort1 {
	sorted_min = ORDER stock_vol_nonull BY $1 ASC;
	lim_min = LIMIT sorted_min 10;
	GENERATE FLATTEN(lim_min);
}

/*Sort Volatility in descending order and get the Top 10 Max*/
stock_sort_max = FOREACH stock_sort1 {
        sorted_max = ORDER stock_vol_nonull BY $1 DESC;
        lim_max = LIMIT sorted_max 10;
        GENERATE FLATTEN(lim_max);
}

stock_final = UNION stock_sort_max, stock_sort_min;

STORE stock_final INTO 'hdfs:///pigdata/hw3_out';

