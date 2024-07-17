/*This program is part of a larger project that uses macros to examine healthcare utilization rates with risk adjustment*/

/*05_2 Utilization Measures_RA*/

proc datasets lib=work kill;
run;

/* Start timer */
%let _timer_start = %sysfunc(datetime());


/*THESE ARE THE SAME FOR ALL UTIL MEASURES*/
%let source_id=4;
%let product_type='ALL';
%let measure_category_num=3;
%let measure_category='Utilization';
%let group_num=1;
%let group='STATEWIDE';
%let hsa_num=1;
%let hsa='ALL';

/*prep PQI92 dataset*/
DATA PQI92 ; 
set jdodge.samp ;
pqi92_inf=coalesce(pqi92_count,0)/mm_ave;
where  age>=18;
run;
/**************************************
Capping , Inflation , Risk Adjustment 
***************************************/
data samp (keep=internal_member_id year member_hsa age bp_prac mat_analysis_group whi mm_ave product_type
			age0117_female age1824_female age2534_female age3544_female age4554_female age5564_female age6574_female age7589_female age90plus_female
			age0117_male age2534_male age3544_male age4554_male age5564_male age6574_male age7589_male age90plus_male
			product_type flg_maternity chronic dual_elig disabled esrd acg_risk_normalized 
			primary_care_visits ip_stays MED_SPEC_VISITS SURG_SPEC_VISITS ED_VISITS AVOID_ED_VISITS
			);
set jdodge.samp;
run;

proc sort data= samp;
by year product_type;
run;

%macro capra(var=,measure_name=);

/*Cap */
proc univariate data=samp;
where &var. > 0 ;
by year product_type;
var &var.;
output out=caplvl p99=p99;
run;

proc sql;
create table capping_&var. as
select my.*, c.p99
	from samp		my
	join caplvl			c 	on my.year = c.year
							and my.product_type = c.product_type
;
quit;

data capping_&var.; set capping_&var.;
if &var. < 0 then do;
	&var._dif = &var.;
	cap_&var. = 0;
end; 
else if &var. > p99 then do;
	&var._dif = &var. - p99;
	cap_&var. = p99;
end;
else do;
	&var._dif = 0;
	cap_&var. = &var.;
end;
/*Inflate*/
cap_&var._inf= cap_&var./mm_ave;
run;

/*risk-adjust*/
proc means data=capping_&var. noprint nway ;
	by year;
	var cap_&var._inf;
	weight mm_ave;
	output out=grandmean mean=grandmean;
run;


proc genmod data=capping_&var.;
	by year;
	class product_type(ref="COMMERCIAL")
		dual_elig(ref="0") flg_maternity(ref="0") disabled(ref="0") esrd(ref="0") chronic(ref="0") ;
	model cap_&var._inf = age0117_female age1824_female age2534_female age3544_female age4554_female age5564_female age6574_female age7589_female age90plus_female
			age0117_male age2534_male age3544_male age4554_male age5564_male age6574_male age7589_male age90plus_male
			product_type flg_maternity chronic dual_elig disabled esrd 
			acg_risk_normalized / dist=poisson link=log; 
	weight mm_Ave; 
	output out=util_ra predicted=pred resraw=resid upper=ucl lower=lcl;
run;

data adjust_&var.;
	merge util_ra (in= a)
		  grandmean (in=b);
	by year;
	if a;
	adj_var = grandmean+resid;
	label year = "Year";
run;

proc datasets lib=work;
delete util_ra grandmean;
run; quit;
%mend;

%capra(var=ip_stays,measure_name='Inpatient Discharges / 1000 Member Years' );
%capra(var=primary_care_visits,measure_name='Primary Care Encounters / 1000 Member Years');
%capra(var=med_spec_visits,measure_name='Medical Specialist Encounters / 1000 Member Years');
%capra(var=surg_spec_visits,measure_name='Surgical Specialist Encounters / 1000 Member Years');
%capra(var=ed_visits,measure_name='Outpatient ED Visits / 1000 Member Years');
%capra(var=avoid_ed_visits,measure_name='Outpatient Potentially Avoidable ED Visits / 1000 Member Years');

/*restrict to requested years*/
data adjust_primary_care_visits;
set adjust_primary_care_visits;
where year in (2013,2019);
run;

data adjust_med_spec_visits;
set adjust_med_spec_visits;
where year in (2013,2019);
run;

data adjust_surg_spec_visits;
set adjust_surg_spec_visits;
where year in (2013,2019);
run;

/*************************************
Calculations
*************************************/

%macro per1000(data=,measure_type_num=,measure_type=,measure_num=,measure_name=,meas=);

/**************************************
Part 1: Total VHCURES - all subpops
***************************************/
proc means data=&data. nway noprint;
      class year ;
      var adj_var;
      weight mm_ave;
      output out=full_pop mean=adj_var_mean sum=adj_Var_sum;
run;
proc means data=&data. nway noprint;
      class year ;
      var mm_ave;
      output out=denom sum=avg_mem ;
run;


DATA part1_4 (drop= numerator _TYPE_ _FREQ_ adj_var_sum adj_var_mean avg_mem);
      MERGE full_pop(in = a) denom (in = b);
	  bp_group=4;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=1;
	  subpopulation='ALL';
      numerator = adj_var_sum;
      if numerator < 11 then do;
           numerator = .;
           adj_var_mean = .;
      end;
	  N=avg_mem;
      rate = adj_var_mean*1000;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  if lcl<0 then lcl=0;
	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

RUN;

proc datasets lib=work;
delete full_pop denom;
run;

/*************************************
BP prac - all subpops
*************************************/
%macro pt1(bp_prac=,bp_group=,out=);
proc means data=&data. nway noprint;
      class year;
      var adj_var;
      weight mm_ave;
	  where bp_prac=&bp_prac.;
      output out=full_pop mean=adj_var_mean sum=adj_Var_sum;
run;
proc means data=&data. nway noprint;
      class year;
      var mm_ave;
	  where bp_prac=&bp_prac.;
      output out=denom sum=avg_mem ;
run;


DATA &out. (drop= numerator _TYPE_ _FREQ_ adj_var_sum adj_var_mean avg_mem);
      MERGE full_pop(in = a) denom (in = b);
	  bp_group=&bp_group.;
	  bp_group_name=put(&bp_group.,gp.) ;
	  subpopulation_num=1;
	  subpopulation='ALL';
      numerator = adj_var_sum;
      if numerator < 11 then do;
           numerator = .;
           adj_var_mean = .;
      end;
	  N=avg_mem;
      rate = adj_var_mean*1000;

      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  if lcl<0 then lcl=0;
	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

RUN;

proc datasets lib=work;
delete full_pop denom;
run;
%mend;
%pt1(out=part1_1,bp_group=1,bp_prac=1); /*PCMH attributed*/
%pt1(out=part1_2,bp_group=2,bp_prac=0); /*Other attributed*/
%pt1(out=part1_3,bp_group=3,bp_prac=-1); /*No primary care attributed*/

data part1;
length  bp_group_name subpopulation $100;
set part1_1 part1_2 part1_3 part1_4;
run;



/******************************************
Part 2: Total VHCURES - OUD groups 
******************************************/
%macro pt2(out=,mat=,subpopnum=);
proc means data=&data. nway noprint;
      class year;
      var adj_var;
      weight mm_ave;
	  where mat_analysis_group=&mat.;
      output out=full_pop mean=adj_var_mean sum=adj_Var_sum;
run;
proc means data=&data. nway noprint;
      class year;
      var mm_ave;
	  where mat_analysis_group=&mat.;
      output out=denom sum=avg_mem ;
run;

DATA &out. (drop= numerator _TYPE_ _FREQ_ adj_var_sum adj_var_mean avg_mem);
      MERGE full_pop(in = a) denom (in = b);
	  bp_group=4;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=&subpopnum.;
	  subpopulation=put(&subpopnum.,subpop.);
      numerator = adj_var_sum;
      if numerator < 11 then do;
           numerator = .;
           adj_var_mean = .;
      end;
	  N=avg_mem;
      rate = adj_var_mean*1000;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  if lcl<0 then lcl=0;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

RUN;

proc datasets lib=work;
delete full_pop denom;
run;
%mend;
%pt2(out=part2_1,subpopnum=2,mat='OUD Dx & MAT');
%pt2(out=part2_2,subpopnum=3,mat='OUD Dx & Other SUD Tx');


data part2;
length bp_group_name subpopulation $100;
set part2_1 part2_2;
run;


/**************************************
Part 3: By BP prac OUD groups 
***************************************/

%macro pt3(out=,mat=,subpopnum=,bp_prac=,bp_group=);
proc means data=&data. nway noprint;
      class year;
      var adj_var;
      weight mm_ave;
	  where bp_prac=&bp_prac. and mat_analysis_group=&mat.;
      output out=full_pop mean=adj_var_mean sum=adj_Var_sum;
run;
proc means data=&data. nway noprint;
      class year;
      var mm_ave;
	  where bp_prac=&bp_prac. and mat_analysis_group=&mat.;
      output out=denom sum=avg_mem ;
run;

DATA &out. (drop= numerator _TYPE_ _FREQ_ adj_var_sum adj_var_mean avg_mem);
      MERGE full_pop(in = a) denom (in = b);
	  bp_group=&bp_group.;
	  bp_group_name=put(&bp_group.,gp.) ;
	  subpopulation_num=&subpopnum.;
	  subpopulation=put(&subpopnum.,subpop.);
      numerator = adj_var_sum;
      if numerator < 11 then do;
           numerator = .;
           adj_var_mean = .;
      end;
	  N=avg_mem;
      rate = adj_var_mean*1000;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  if lcl<0 then lcl=0;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

RUN;


proc datasets lib=work;
delete full_pop denom;
run;

%mend;
/*PCMH primary care attributed*/
%pt3(out=part3_1,subpopnum=2,mat='OUD Dx & MAT',bp_prac=1,bp_group=1);
%pt3(out=part3_2,subpopnum=3,mat='OUD Dx & Other SUD Tx',bp_prac=1,bp_group=1);

/*Other primary care attributed*/
%pt3(out=part3_3,subpopnum=2,mat='OUD Dx & MAT',bp_prac=0,bp_group=2);
%pt3(out=part3_4,subpopnum=3,mat='OUD Dx & Other SUD Tx',bp_prac=0,bp_group=2);

/*No primary care attributed*/
%pt3(out=part3_5,subpopnum=2,mat='OUD Dx & MAT',bp_prac=-1,bp_group=3);
%pt3(out=part3_6,subpopnum=3,mat='OUD Dx & Other SUD Tx',bp_prac=-1,bp_group=3);

data part3;
length bp_group_name subpopulation $100;
set part3_1 part3_2 part3_3 part3_4 part3_5 part3_6;
run;


proc datasets lib=work;
delete full_pop denom;
run;
/**************************************
Part 4: WHI
***************************************/

/*Total VHCURES*/

proc means data=&data. nway noprint;
      class year ;
      var adj_var;
      weight mm_ave;
	where whi=1;
	output out=full_pop mean=adj_var_mean sum=adj_Var_sum;
run;
proc means data=&data. nway noprint;
      class year ;
      var mm_ave;
where whi=1;
      output out=denom sum=avg_mem ;
run;


DATA part4_4 (drop= numerator _TYPE_ _FREQ_ adj_var_sum adj_var_mean avg_mem);
      MERGE full_pop(in = a) denom (in = b);
	  bp_group=4;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=4;
	  subpopulation='All-Payer Women Ages 15-44';
      numerator = adj_var_sum;
      if numerator < 11 then do;
           numerator = .;
           adj_var_mean = .;
      end;
	  N=avg_mem;
      rate = adj_var_mean*1000;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  if lcl<0 then lcl=0;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

RUN;


proc datasets lib=work;
delete full_pop denom;
run;
/*************************************
BP prac
*************************************/
%macro pt4(bp_prac=, out=,bp_group=);

proc means data=&data. nway noprint;
      class year ;
      var adj_var;
      weight mm_ave;
where whi=1 and bp_prac=&bp_prac.;
output out=full_pop mean=adj_var_mean sum=adj_Var_sum;
run;
proc means data=&data. nway noprint;
      class year ;
      var mm_ave;
where whi=1 and bp_prac=&bp_prac.;
output out=denom sum=avg_mem ;
run;


DATA &out. (drop= numerator _TYPE_ _FREQ_ adj_var_sum adj_var_mean avg_mem);
      MERGE full_pop(in = a) denom (in = b);
	  bp_group=&bp_group.;
	  bp_group_name=put(&bp_group.,gp.) ;
	  subpopulation_num=4;
	  subpopulation='All-Payer Women Ages 15-44';
      numerator = adj_var_sum;
      if numerator < 11 then do;
           numerator = .;
           adj_var_mean = .;
      end;
	  N=avg_mem;
      rate = adj_var_mean*1000;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))*1000/avg_mem;
	  if lcl<0 then lcl=0;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

RUN;


proc datasets lib=work;
delete full_pop denom;
run;

%mend;
%pt4(bp_prac=1, out=part4_1,bp_group=1);/*PCMH primary care attributed*/
%pt4(bp_prac=0, out=part4_2,bp_group=2);/*Other primary care attributed*/
%pt4(bp_prac=-1, out=part4_3,bp_group=3);/*No primary care attributed*/

data part4;
length bp_group_name subpopulation $100;
set part4_1 part4_2 part4_3 part4_4;
run;

data &meas._final;
retain source_id	PRODUCT_TYPE MEASURE_CATEGORY_NUM MEASURE_CATEGORY MEASURE_TYPE_NUM	MEASURE_TYPE MEASURE_NUM MEASURE_NAME YEAR N RATE 
LCL	UCL	GROUP_NUM GROUP	HSA_NUM	HSA	BP_GROUP BP_GROUP_NAME SUBPOPULATION_NUM SUBPOPULATION ;
length  bp_group_name subpopulation $100;
set part1 part2 part3 part4;
if N<11 then do;
	N=.;
	rate=.;
end;
source_id=&source_id.;
product_type=&product_type.;
measure_category_num=&measure_category_num.;
measure_category=&measure_category.;
measure_type_num=&measure_type_num.;
measure_type=&measure_type.;
measure_num=&measure_num.;
measure_name=&measure_name.;
group_num=&group_num.;
group=&group.;
hsa_num=&hsa_num.;
hsa=&hsa.;
run;
/**/
/*proc datasets lib=work;*/
/*delete part1: part2: part3: part4:;*/
/*run;*/

%mend;
%per1000(data=adjust_primary_care_visits,measure_type_num=32,measure_type='Utilization of Primary Care',measure_num=46,measure_name='Primary Care Encounters / 1000 Member Years',meas=pc);
%per1000(data=adjust_ip_stays,measure_type_num=12,measure_type='Inpatient Discharges',measure_num=35,measure_name='Inpatient Discharges / 1000 Member Years',meas=ip);
%per1000(data=adjust_med_spec_visits,measure_type_num=31,measure_type='Utilization of Other Care',measure_num=44,measure_name='Medical Specialist Encounters / 1000 Member Years',meas=med);
%per1000(data=adjust_surg_spec_visits,measure_type_num=31,measure_type='Utilization of Other Care',measure_num=45,measure_name='Surgical Specialist Encounters / 1000 Member Years',meas=surg);
%per1000(data=adjust_ed_visits,measure_type_num=7,measure_type='Emergency Department Visits',measure_num=39,measure_name='Outpatient ED Visits / 1000 Member Years',meas=op_ed);
%per1000(data=adjust_avoid_ed_visits,measure_type_num=7,measure_type='Emergency Department Visits',measure_num=40,measure_name='Outpatient Potentially Avoidable ED Visits / 1000 Member Years',meas=avoid_ed);


/*************************************
PQI92 
*************************************/


%include "\\aws-coremgmt-filer1.OPHD.onpointhd.org\PCCOMMON\Clients\vt\VT DVHA (VTVHA)\Analytics\Scheduled\Year End Evaluation - CY19\Code\Annual Evaluation Measures\09 PQIs.sas";

%pqi1(data=PQI92, var=pqi92_inf, summary=mean,meas_cat_num=3
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_total);
%pqi2(data=PQI92, var=pqi92_inf, bp_prac=1,bp_group=1,summary=mean,meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_1);
%pqi2(data=PQI92, var=pqi92_inf, bp_prac=0,bp_group=2,summary=mean,meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_2);
%pqi2(data=PQI92, var=pqi92_inf, bp_prac=-1,bp_group=3,summary=mean,meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_3);
%pqi3(data=PQI92, var=pqi92_inf, mat='OUD Dx & MAT',subpop_num=2, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_3_1);
%pqi3(data=PQI92, var=pqi92_inf, mat='OUD Dx & Other SUD Tx',subpop_num=3, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_3_2);
%pqi4(data=PQI92, var=pqi92_inf, mat='OUD Dx & MAT',bp_prac=1,bp_group=1,subpop_num=2, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_mat_pcmh);
%pqi4(data=PQI92, var=pqi92_inf, mat='OUD Dx & Other SUD Tx',bp_prac=1,bp_group=1,subpop_num=3, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_other_pcmh);
%pqi4(data=PQI92, var=pqi92_inf, mat='OUD Dx & MAT',bp_prac=0,bp_group=2,subpop_num=2, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_mat_other);
%pqi4(data=PQI92, var=pqi92_inf, mat='OUD Dx & Other SUD Tx',bp_prac=0,bp_group=2,subpop_num=3, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_other_other);
%pqi4(data=PQI92, var=pqi92_inf, mat='OUD Dx & MAT',bp_prac=-1,bp_group=3,subpop_num=2, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_mat_not);
%pqi4(data=PQI92, var=pqi92_inf, mat='OUD Dx & Other SUD Tx',bp_prac=-1,bp_group=3,subpop_num=3, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_other_not);
%pqi5(data=PQI92, var=pqi92_inf, subpop_num=4, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_5);
%pqi6(data=PQI92, var=pqi92_inf, bp_prac=1,bp_group=1,subpop_num=4, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_6_1);
%pqi6(data=PQI92, var=pqi92_inf, bp_prac=0,bp_group=2,subpop_num=4, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_6_2);
%pqi6(data=PQI92, var=pqi92_inf, bp_prac=-1,bp_group=3,subpop_num=4, meas_cat_num=4
,measure_cat='Utilization',measure_type_num=12,measure_type='Inpatient Discharges',measure_num=125
,measure_name='PQI92 Chronic Composite ACSC IP Discharges / 1000 Member Years'
,out=pqi92_6_3);

data pqi_92_final;
retain source_id	PRODUCT_TYPE MEASURE_CATEGORY_NUM MEASURE_CATEGORY MEASURE_TYPE_NUM	MEASURE_TYPE MEASURE_NUM MEASURE_NAME YEAR N RATE 
LCL	UCL	GROUP_NUM GROUP	HSA_NUM	HSA	BP_GROUP BP_GROUP_NAME SUBPOPULATION_NUM SUBPOPULATION ;
length  measure_category measure_type measure_name bp_group_name subpopulation $100;
set pqi92_total pqi92_1 pqi92_2  pqi92_3 pqi92_3_1 pqi92_3_2 pqi92_mat_pcmh  pqi92_other_pcmh  pqi92_mat_other  pqi92_other_other pqi92_mat_not  pqi92_other_not
pqi92_5 pqi92_6_1 pqi92_6_2 pqi92_6_3;
run;



data pcr;
set jdodge.samp (keep=internal_member_id year age pcr_readmissions pcr_expected_readmissions mm_ave bp_prac mat_analysis_group whi member_hsa );
where year in (2013,2019) and age>=18;
run;


/**************************************
Part 1: Total VHCURES - all subpops
***************************************/

proc means data= pcr nway noprint;
      class year ;
      var pcr_readmissions;
      weight mm_ave;
      output out=full_pop sum=pcr_num;
run;
proc means data= pcr  nway noprint;
      class year ;
      var pcr_expected_readmissions;
	  output out=denom sum=pcr_exp ;
run;


DATA part1_4 (drop= _TYPE_ _FREQ_ pcr_exp pcr_num);
      MERGE full_pop(in = a) denom (in = b);
	  by year;
      N=pcr_num;
	  rate=pcr_num/pcr_exp;
	  lcl=(pcr_num-1.96*SQRT(pcr_num))/pcr_exp;
	  ucl=(pcr_num+1.96*SQRT(pcr_num))/pcr_exp;
	   if N < 11 then do;
           N = .;
           rate = .;
		   LCL=.;
		   UCL=.;
      end;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

	  bp_group=4;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=1;
	  subpopulation='ALL';
 run;
/*************************************
BP prac - all subpops
*************************************/

%macro pt1(bp_prac=,bp_group=,out=);
proc means data=pcr nway noprint;
      class year ;
      var pcr_readmissions;
      weight mm_ave;
	  where bp_prac=&bp_prac.;
      output out=full_pop sum=pcr_num;
run;
proc means data= pcr  nway noprint;
      class year ;
      var pcr_expected_readmissions;
	  where bp_prac=&bp_prac.;
	  output out=denom sum=pcr_exp ;
run;


DATA &out. (drop= _TYPE_ _FREQ_ pcr_exp pcr_num);
      MERGE full_pop(in = a) denom (in = b);
	  by year;
      N=pcr_num;
	  rate=pcr_num/pcr_exp;
	  lcl=(pcr_num-1.96*SQRT(pcr_num))/pcr_exp;
	  ucl=(pcr_num+1.96*SQRT(pcr_num))/pcr_exp;
	   if N < 11 then do;
           N = .;
           rate = .;
		   LCL=.;
		   UCL=.;
      end;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

	bp_group=&bp_group.;
	  bp_group_name=put(&bp_group.,gp.) ;
	  subpopulation_num=1;
	  subpopulation='ALL';
run;

 %mend;

%pt1(out=part1_1,bp_group=1,bp_prac=1); /*PCMH attributed*/
%pt1(out=part1_2,bp_group=2,bp_prac=0); /*Other attributed*/
%pt1(out=part1_3,bp_group=3,bp_prac=-1); /*No primary care attributed*/

data part1;
length  bp_group_name subpopulation $100;
set part1_1 part1_2 part1_3 part1_4;
run;


/******************************************
Part 2: Total VHCURES - OUD groups 
******************************************/
%macro pt2(mat=,subpopnum=,out=);
proc means data=pcr nway noprint;
      class year ;
      var pcr_readmissions;
      weight mm_ave;
	  where mat_analysis_group=&mat.;
      output out=full_pop sum=pcr_num;
run;
proc means data= pcr  nway noprint;
      class year ;
      var pcr_expected_readmissions;
	   where mat_analysis_group=&mat.;
		output out=denom sum=pcr_exp ;
run;


DATA &out. (drop= _TYPE_ _FREQ_ pcr_exp pcr_num);
       MERGE full_pop(in = a) denom (in = b);
	  by year;
      N=pcr_num;
	  rate=pcr_num/pcr_exp;
	  lcl=(pcr_num-1.96*SQRT(pcr_num))/pcr_exp;
	  ucl=(pcr_num+1.96*SQRT(pcr_num))/pcr_exp;
	  if N < 11 then do;
           N = .;
           rate = .;
		   LCL=.;
		   UCL=.;
      end;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

		bp_group=4;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=&subpopnum.;
	  subpopulation=put(&subpopnum.,subpop.);

 run;

%mend;

%pt2(out=part2_1,subpopnum=2,mat='OUD Dx & MAT');
%pt2(out=part2_2,subpopnum=3,mat='OUD Dx & Other SUD Tx');

data part2;
length bp_group_name subpopulation $100;
set part2_1 part2_2;
run;


/**************************************
Part 3: By BP prac OUD groups 
***************************************/
%macro pt3(bp_group=,bp_prac=,mat=,subpopnum=,out=);
proc means data=pcr nway noprint;
      class year ;
      var pcr_readmissions;
      weight mm_ave;
	  where bp_prac=&bp_prac. and mat_analysis_group=&mat.;
      output out=full_pop sum=pcr_num;
run;
proc means data= pcr  nway noprint;
      class year ;
      var pcr_expected_readmissions;
	  where bp_prac=&bp_prac. and mat_analysis_group=&mat.;
	  output out=denom sum=pcr_exp ;
run;


DATA &out. (drop= _TYPE_ _FREQ_ pcr_exp pcr_num);
       MERGE full_pop(in = a) denom (in = b);
	  by year;
      N=pcr_num;
	  rate=pcr_num/pcr_exp;
	  lcl=(pcr_num-1.96*SQRT(pcr_num))/pcr_exp;
	  ucl=(pcr_num+1.96*SQRT(pcr_num))/pcr_exp;
	  if N < 11 then do;
           N = .;
           rate = .;
		   LCL=.;
		   UCL=.;
      end;	  
	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;
bp_group=&bp_group.;
	  bp_group_name=put(&bp_group.,gp.) ;
	  subpopulation_num=&subpopnum.;
	  subpopulation=put(&subpopnum.,subpop.);

 run;

%mend;

/*PCMH primary care attributed*/
%pt3(out=part3_1,subpopnum=2,mat='OUD Dx & MAT',bp_prac=1,bp_group=1);
%pt3(out=part3_2,subpopnum=3,mat='OUD Dx & Other SUD Tx',bp_prac=1,bp_group=1);

/*Other primary care attributed*/
%pt3(out=part3_3,subpopnum=2,mat='OUD Dx & MAT',bp_prac=0,bp_group=2);
%pt3(out=part3_4,subpopnum=3,mat='OUD Dx & Other SUD Tx',bp_prac=0,bp_group=2);

/*No primary care attributed*/
%pt3(out=part3_5,subpopnum=2,mat='OUD Dx & MAT',bp_prac=-1,bp_group=3);
%pt3(out=part3_6,subpopnum=3,mat='OUD Dx & Other SUD Tx',bp_prac=-1,bp_group=3);

data part3;
length bp_group_name subpopulation $100;
set part3_1 part3_2 part3_3 part3_4 part3_5 part3_6;
run;



/**************************************
Part 4: WHI
***************************************/

/*Total VHCURES*/

proc means data=pcr nway noprint;
      class year ;
      var pcr_readmissions;
      weight mm_ave;
where whi=1;
output out=full_pop sum=pcr_num;
run;
proc means data= pcr  nway noprint;
      class year ;
      var pcr_expected_readmissions;
where whi=1;		output out=denom sum=pcr_num ;
run;



DATA part4_4 (drop= _TYPE_ _FREQ_ pcr_exp pcr_num);
       MERGE full_pop(in = a) denom (in = b);
	  by year;
      N=pcr_num;
	  rate=pcr_num/pcr_exp;
	  lcl=(pcr_num-1.96*SQRT(pcr_num))/pcr_exp;
	  ucl=(pcr_num+1.96*SQRT(pcr_num))/pcr_exp;
	  if N < 11 then do;
           N = .;
           rate = .;
		   LCL=.;
		   UCL=.;
      end;
	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;
 
		bp_group=4;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=4.;
	  subpopulation=put(subpopulation_num,subpop.);

 run;


/*************************************
BP prac
*************************************/
%macro pt4(bp_prac=, out=,bp_group=);


proc means data=pcr nway noprint;
      class year ;
      var pcr_readmissions;
      weight mm_ave;
where whi=1;
output out=full_pop sum=pcr_num;
run;

proc means data= pcr  nway noprint;
      class year ;
      var pcr_expected_readmissions;
where whi=1;
output out=denom sum=pcr_exp ;
run;

DATA &out. (drop= _TYPE_ _FREQ_ pcr_exp pcr_num);
       MERGE full_pop(in = a) denom (in = b);
	  by year;
      N=pcr_num;
	  rate=pcr_num/pcr_exp;
	  lcl=(pcr_num-1.96*SQRT(pcr_num))/pcr_exp;
	  ucl=(pcr_num+1.96*SQRT(pcr_num))/pcr_exp;
	  if N < 11 then do;
           N = .;
           rate = .;
		   LCL=.;
		   UCL=.;
      end;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

	bp_group=&bp_group.;
	  bp_group_name=put(&bp_group.,gp.) ;
	  subpopulation_num=4.;
	  subpopulation=put(subpopulation_num,subpop.);

 run;


%mend;
%pt4(bp_prac=1, out=part4_1,bp_group=1);/*PCMH primary care attributed*/
%pt4(bp_prac=0, out=part4_2,bp_group=2);/*Other primary care attributed*/
%pt4(bp_prac=-1, out=part4_3,bp_group=3);/*No primary care attributed*/

data part4;
length bp_group_name subpopulation $100;
set part4_1 part4_2 part4_3 part4_4;
run;


data pcr_final;
retain source_id PRODUCT_TYPE MEASURE_CATEGORY_NUM MEASURE_CATEGORY MEASURE_TYPE_NUM	MEASURE_TYPE MEASURE_NUM MEASURE_NAME YEAR N RATE 
LCL	UCL	GROUP_NUM GROUP	HSA_NUM	HSA	BP_GROUP BP_GROUP_NAME SUBPOPULATION_NUM SUBPOPULATION ;
length  bp_group_name subpopulation $100;
set part1 part2 part3 part4;
if N<11 then do;
	N=.;
	rate=.;
	LCL=.;
	UCL=.;
end;
source_id=&source_id.;
product_type=&product_type.;
measure_category_num=&measure_category_num.;
measure_category=&measure_category.;
measure_type_num=12.;
measure_type='Inpatient Discharges';
measure_num=38;
measure_name='Rate of All-Cause Hospital Readmissions within 30 Days (%)';
group_num=&group_num.;
group=&group.;
hsa_num=&hsa_num.;
hsa=&hsa.;
run;


/*************************************
% with Primary Care Visit in Year
*************************************/

%let measure_type_num=32;
%let measure_type='Utilization of Primary Care';
%let measure_num=43;
%let measure_name='% with Primary Care Visit in Year';

/*************************************
Part 1: Total VHCURES - all subpops
*************************************/

proc sql;
create table meas_util as
select internal_member_id,year, mat_analysis_group, whi, bp_prac,max((adj_var>0)) as util_meas, sum(mm_Ave) as mm_ave
	from adjust_primary_care_visits 
	group by internal_member_id,year, mat_analysis_group, whi, bp_prac
;
QUIT;

proc means data=meas_util nway noprint;
      class year ;
      var util_meas;
      weight mm_ave;
      output out=full_pop sum=adj_Var_sum;
run;
proc means data=meas_util nway noprint;
      class year ;
      var mm_ave;
	  output out=denom sum=avg_mem ;
run;


DATA part1_4 (drop= _TYPE_ _FREQ_ avg_mem adj_Var_sum);
      MERGE full_pop(in = a) denom (in = b);
	  by year;
	  N=avg_mem;
      rate = adj_Var_sum/avg_mem;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))/avg_mem;
	  if adj_Var_sum < 11 then do;
           adj_Var_sum = .;
           rate = .;
		   LCL=.;
		   UCL=.;
	end;
	  if lcl<0 then lcl=0;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

	  bp_group=4;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=1;
	  subpopulation='ALL';

RUN;




/*************************************
BP prac - all subpops
*************************************/
%macro pt1(bp_prac=,bp_group=,out=);


proc means data=meas_util nway noprint;
      class year ;
      var util_meas;
      weight mm_ave;
	  where bp_prac=&bp_prac.;
      output out=full_pop sum=adj_Var_sum;
run;
proc means data=meas_util nway noprint;
      class year ;
      var mm_ave;
	  where bp_prac=&bp_prac.;
	  output out=denom sum=avg_mem ;
run;

DATA &out. (drop= _TYPE_ _FREQ_ avg_mem adj_Var_sum);
      MERGE full_pop(in = a) denom (in = b);
	  by year;
      if adj_Var_sum < 11 then do;
           adj_Var_sum = .;
           rate = .;
      end;
	  N=avg_mem;
      rate = adj_Var_sum/avg_mem;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))/avg_mem;
	  if lcl<0 then lcl=0;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;

	  bp_group=&bp_group.;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=1;
	  subpopulation='ALL';

RUN;
%mend;
%pt1(out=part1_1,bp_group=1,bp_prac=1); /*PCMH attributed*/
%pt1(out=part1_2,bp_group=2,bp_prac=0); /*Other attributed*/
%pt1(out=part1_3,bp_group=3,bp_prac=-1); /*No primary care attributed*/

data part1;
length  bp_group_name subpopulation $100;
set part1_1 part1_2 part1_3 part1_4;
run;
/******************************************
Part 2: Total VHCURES - OUD groups 
******************************************/
%macro pt2(mat=,subpopnum=,out=);

proc means data=meas_util nway noprint;
      class year ;
      var util_meas;
      weight mm_ave;
	  where mat_analysis_group=&mat.;
      output out=full_pop sum=adj_Var_sum;
run;
proc means data=meas_util nway noprint;
      class year ;
      var mm_ave;
	  where mat_analysis_group=&mat.;
	  output out=denom sum=avg_mem ;
run;


DATA &out. (drop= _TYPE_ _FREQ_ avg_mem adj_Var_sum);
      MERGE full_pop(in = a) denom (in = b);
	  by year;
      if adj_Var_sum < 11 then do;
           adj_Var_sum = .;
           rate = .;
      end;
	  N=avg_mem;
      rate = adj_Var_sum/avg_mem;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))/avg_mem;
	  	  if lcl<0 then lcl=0;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;
	  bp_group=4;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=&subpopnum.;
	  subpopulation=put(&subpopnum.,subpop.);

RUN;
%mend;
%pt2(out=part2_1,subpopnum=2,mat='OUD Dx & MAT');
%pt2(out=part2_2,subpopnum=3,mat='OUD Dx & Other SUD Tx');

data part2;
length bp_group_name subpopulation $100;
set part2_1 part2_2;
run;
/**************************************
Part 3: By BP prac OUD groups 
***************************************/
%macro pt3(bp_group=,bp_prac=,mat=,subpopnum=,out=);

proc means data=meas_util nway noprint;
      class year ;
      var util_meas;
      weight mm_ave;
	  where bp_prac=&bp_prac. and mat_analysis_group=&mat.;
      output out=full_pop sum=adj_Var_sum;
run;
proc means data=meas_util nway noprint;
      class year ;
      var mm_ave;
	  where bp_prac=&bp_prac. and mat_analysis_group=&mat.;
	  output out=denom sum=avg_mem ;
run;

DATA &out. (drop= _TYPE_ _FREQ_ avg_mem adj_Var_sum);
      MERGE full_pop(in = a) denom (in = b);
	  by year;
      if adj_Var_sum < 11 then do;
           adj_Var_sum = .;
           rate = .;
      end;
	  N=avg_mem;
      rate = adj_Var_sum/avg_mem;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))/avg_mem;
	  	  if lcl<0 then lcl=0;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;
	  bp_group=&bp_group.;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=&subpopnum.;
	  subpopulation=put(&subpopnum.,subpop.);

RUN;
%mend;
/*PCMH primary care attributed*/
%pt3(out=part3_1,subpopnum=2,mat='OUD Dx & MAT',bp_prac=1,bp_group=1);
%pt3(out=part3_2,subpopnum=3,mat='OUD Dx & Other SUD Tx',bp_prac=1,bp_group=1);

/*Other primary care attributed*/
%pt3(out=part3_3,subpopnum=2,mat='OUD Dx & MAT',bp_prac=0,bp_group=2);
%pt3(out=part3_4,subpopnum=3,mat='OUD Dx & Other SUD Tx',bp_prac=0,bp_group=2);

/*No primary care attributed*/
%pt3(out=part3_5,subpopnum=2,mat='OUD Dx & MAT',bp_prac=-1,bp_group=3);
%pt3(out=part3_6,subpopnum=3,mat='OUD Dx & Other SUD Tx',bp_prac=-1,bp_group=3);

data part3;
length bp_group_name subpopulation $100;
set part3_1 part3_2 part3_3 part3_4 part3_5 part3_6;
run;

/**************************************
Part 4: WHI
***************************************/

proc means data=meas_util nway noprint;
      class year ;
      var util_meas;
      weight mm_ave;
where whi=1;
output out=full_pop sum=adj_Var_sum;
run;
proc means data=meas_util nway noprint;
      class year ;
      var mm_ave;
where whi=1;
output out=denom sum=avg_mem ;
run;


DATA part4_4 (drop= _TYPE_ _FREQ_ avg_mem adj_Var_sum);
      MERGE full_pop(in = a) denom (in = b);
	  by year;
      if adj_Var_sum < 11 then do;
           adj_Var_sum = .;
           rate = .;
      end;
	  N=avg_mem;
      rate = adj_Var_sum/avg_mem;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))/avg_mem;
	  	  if lcl<0 then lcl=0;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;
	  bp_group=4;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=4;
	  subpopulation=put(subpopulation_num,subpop.);

RUN;

/*BP prac*/
%macro pt4(bp_prac=, out=,bp_group=);


proc means data=meas_util nway noprint;
      class year ;
      var util_meas;
      weight mm_ave;
where whi=1;
output out=full_pop sum=adj_Var_sum;
run;
proc means data=meas_util nway noprint;
      class year ;
      var mm_ave;
where whi=1;
output out=denom sum=avg_mem ;
run;


DATA &out. (drop= _TYPE_ _FREQ_ avg_mem adj_Var_sum	);
      MERGE full_pop(in = a) denom (in = b);
	  by year;
      if adj_Var_sum < 11 then do;
           adj_Var_sum = .;
           rate = .;
      end;
	  N=avg_mem;
      rate = adj_Var_sum/avg_mem;
      lcl=(adj_var_sum-1.96*SQRT(adj_var_sum))/avg_mem;
	  ucl=(adj_var_sum+1.96*SQRT(adj_var_sum))/avg_mem;
	  	  if lcl<0 then lcl=0;
	  	  if rate<0 then do;
			rate=0;
			lcl=0;
			ucl=0;
			end;
	  bp_group=&bp_group.;
	  bp_group_name=put(bp_group,gp.) ;
	  subpopulation_num=4;
	  subpopulation=put(subpopulation_num,subpop.);

RUN;

%mend;
%pt4(bp_prac=1, out=part4_1,bp_group=1);/*PCMH primary care attributed*/
%pt4(bp_prac=0, out=part4_2,bp_group=2);/*Other primary care attributed*/
%pt4(bp_prac=-1, out=part4_3,bp_group=3);/*No primary care attributed*/
data part4;
length bp_group_name subpopulation $100;
set part4_1 part4_2 part4_3 part4_4;
run;



data pct_pc_final;
retain source_id	PRODUCT_TYPE MEASURE_CATEGORY_NUM MEASURE_CATEGORY MEASURE_TYPE_NUM	MEASURE_TYPE MEASURE_NUM MEASURE_NAME YEAR N RATE 
LCL	UCL	GROUP_NUM GROUP	HSA_NUM	HSA	BP_GROUP BP_GROUP_NAME SUBPOPULATION_NUM SUBPOPULATION ;
length measure_name bp_group_name subpopulation $100;
set part1 part2 part3 part4;
if N<11 then do;
	N=.;
	rate=.;
end;
source_id=&source_id.;
product_type=&product_type.;
measure_category_num=&measure_category_num.;
measure_category=&measure_category.;
measure_type_num=&measure_type_num.;
measure_type=&measure_type.;
measure_num=&measure_num.;
measure_name=&measure_name.;
group_num=&group_num.;
group=&group.;
hsa_num=&hsa_num.;
hsa=&hsa.;
run;


data util_ra;
retain source_id	PRODUCT_TYPE MEASURE_CATEGORY_NUM MEASURE_CATEGORY MEASURE_TYPE_NUM	MEASURE_TYPE MEASURE_NUM MEASURE_NAME YEAR N RATE 
LCL	UCL	GROUP_NUM GROUP	HSA_NUM	HSA	BP_GROUP BP_GROUP_NAME SUBPOPULATION_NUM SUBPOPULATION ;
length measure_name bp_group_name subpopulation $100;
set pc_final 
ip_final 
med_final 
surg_final 
op_ed_final 
avoid_ed_final 
pqi_92_final 
pcr_final 
pct_pc_final ;
run;


data yee.util_ra;
set util_ra;
run;

proc freq data=yee.util_ra;
tables measure_name bp_group_name subpopulation /missing;
run;

/* Stop timer */
data _null_;
  dur = datetime() - &_timer_start;
  put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
run;
