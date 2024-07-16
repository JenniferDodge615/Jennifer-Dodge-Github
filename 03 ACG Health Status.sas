/*proc datasets lib=work kill;*/
/*run;*/

/*%symdel;*/
%include "\\aws-coremgmt-filer1.OPHD.onpointhd.org\PCCOMMON\Clients\vt\VT DVHA (VTVHA)\Analytics\Scheduled\Year End Evaluation - CY19\Code\Annual Evaluation Measures\00 Prep.sas";

/********************************************
ACG HEALTH STATUS (RUB CATEGORIES) VHCURES ALL PRODUCT TYPES
*********************************************/
/*these stay the same for all ACG Breakouts */
%let source_id=4;
%let product_type='ALL';
%let measure_category_num=2;
%let measure_category='Characteristics';
%let measure_type_num=15;
%let measure_type='ACG Health Status';

/*These will change for HSA breakouts*/
%let group_num=1;
%let group='STATEWIDE';
%let hsa_num=1;
%let hsa='ALL';
/**/
data samp (keep=year rub_num bp_prac mat_analysis_group internal_member_id whi mm_ave member_hsa);
set jdodge.samp;
where year in (2013,2019);
rub_num=rub*1;
run;

/********************************************
Part 1: % ACG measures Total VHCURES 
*********************************************/

/*create denominator total VHCURES and all subpops*/

proc summary data=samp NWAY MISSING sum;
class year;
var mm_ave;
output out=base (drop=_TYPE_ _FREQ_) sum=N;
run;


%macro acg1(rub=,measure_num=);

/*numerators by rub*/
proc summary data=samp NWAY MISSING sum;
class year rub_num;
var mm_ave;
where rub_num=&rub.;
output out=num_&rub. (drop=_TYPE_ _FREQ_) sum=N;
run;

/*create rate*/
 proc sql;
 create table rub_&rub. as
 select &measure_num. as measure_num, put(&rub.,rub.) as measure_name, a.year,a.N,a.N/b.N as rate
 from num_&rub. a
 left join base b on a.year=b.year;
 quit;

%mend;

%acg1(rub=1,measure_num=5);
%acg1(rub=2,measure_num=6);
%acg1(rub=3,measure_num=7);
%acg1(rub=4,measure_num=8);
%acg1(rub=5,measure_num=9);


data part1;
length measure_name bp_group_name subpopulation $100;
set rub_1 rub_2 rub_3 rub_4 rub_5;
LCL=.;
UCL=.;
group=&group.;
group_num=&group_num.;
hsa_num=&hsa_num.;
hsa=&hsa.;
bp_group=4;
bp_group_name='Total VHCURES (Excluding Self-Insured)';
subpopulation_num=1;
subpopulation='ALL';
run;


/****************************************************************
Part 2: % ACG measures by attribution group - all subpopulations 
*****************************************************************/

%macro bp_gp(table=,measure_num=,bp_group=,bp_prac=,rub=,out=);

/*denominator*/
proc summary data=samp NWAY MISSING sum;
class year;
var mm_ave;
where bp_prac=&bp_prac.;
output out=&table._denom (drop=_TYPE_ _FREQ_) sum=N;
run;

/*numerator*/
proc summary data=samp NWAY MISSING sum;
class year;
var mm_ave;
where rub_num=&rub. and bp_prac=&bp_prac.;
output out=&table. (drop=_TYPE_ _FREQ_) sum=N;
run;

/*create rate*/
proc sql;
create table &out. as
select 
&measure_num. as measure_num
,put(&rub.,rub.) as measure_name
, a.year
, a.N
, a.N/b.N as rate
, &bp_group. as bp_group
, put(&bp_prac.,bp.) as bp_group_name
from &table. a 
left join &table._denom b on a.year=b.year  ;
quit;
%mend;

/*PCMH Primary Care Attributed*/
%bp_gp(table=pcmh1,measure_num=5,bp_group=1,bp_prac=1,rub=1,out=pcmhf1);
%bp_gp(table=pcmh2,measure_num=6,bp_group=1,bp_prac=1,rub=2,out=pcmhf2);
%bp_gp(table=pcmh3,measure_num=7,bp_group=1,bp_prac=1,rub=3,out=pcmhf3);
%bp_gp(table=pcmh4,measure_num=8,bp_group=1,bp_prac=1,rub=4,out=pcmhf4);
%bp_gp(table=pcmh5,measure_num=9,bp_group=1,bp_prac=1,rub=5,out=pcmhf5);

/*Other Primary Care Attributed*/
%bp_gp(table=other1,measure_num=5,bp_group=2,bp_prac=0,rub=1,out=otherf1);
%bp_gp(table=other2,measure_num=6,bp_group=2,bp_prac=0,rub=2,out=otherf2);
%bp_gp(table=other3,measure_num=7,bp_group=2,bp_prac=0,rub=3,out=otherf3);
%bp_gp(table=other4,measure_num=8,bp_group=2,bp_prac=0,rub=4,out=otherf4);
%bp_gp(table=other5,measure_num=9,bp_group=2,bp_prac=0,rub=5,out=otherf5);

/*No Primary Care Attributed*/
%bp_gp(table=no1,measure_num=5,bp_group=3,bp_prac=-1,rub=1,out=nof1);
%bp_gp(table=no2,measure_num=6,bp_group=3,bp_prac=-1,rub=2,out=nof2);
%bp_gp(table=no3,measure_num=7,bp_group=3,bp_prac=-1,rub=3,out=nof3);
%bp_gp(table=no4,measure_num=8,bp_group=3,bp_prac=-1,rub=4,out=nof4);
%bp_gp(table=no5,measure_num=9,bp_group=3,bp_prac=-1,rub=5,out=nof5);


data part2;
length measure_name bp_group_name subpopulation $100;
set pcmhf1 pcmhf2 pcmhf3 pcmhf4 pcmhf5 otherf1 otherf2 otherf3 otherf4 otherf5 nof1 nof2 nof3 nof4 nof5;
LCL=.;
UCL=.;
group=&group.;
group_num=&group_num.;
hsa_num=&hsa_num.;
hsa=&hsa.;
subpopulation_num=1;
subpopulation='ALL';
run;

/****************************************************************
Part 3: % ACG measures Total VHCURES - MAT subpopulation 
*****************************************************************/

proc summary data=jdodge.samp NWAY MISSING sum;
class year;
var mm_ave;
where mat_analysis_group='OUD Dx & MAT';
output out=mat_base (drop=_TYPE_ _FREQ_) sum=N;
run;

%macro mat(rub=,measure_num=);

/*numerators by rub*/
proc summary data=samp NWAY MISSING sum;
class year rub_num;
var mm_ave;
where rub_num=&rub. and mat_analysis_group='OUD Dx & MAT';
output out=num_&rub. (drop=_TYPE_ _FREQ_) sum=N;
run;

/*create rate*/
 proc sql;
 create table mat_&rub. as
 select &measure_num. as measure_num, put(&rub.,rub.) as measure_name, a.year,a.N,a.N/b.N as rate
 from num_&rub. a
 left join mat_base b on a.year=b.year;
 quit;

%mend;

%mat(rub=1,measure_num=5);
%mat(rub=2,measure_num=6);
%mat(rub=3,measure_num=7);
%mat(rub=4,measure_num=8);
%mat(rub=5,measure_num=9);

data part3;
length measure_name bp_group_name subpopulation $100;
set mat_1 mat_2 mat_3 mat_4 mat_5;
LCL=.;
UCL=.;
group=&group.;
group_num=&group_num.;
hsa_num=&hsa_num.;
hsa=&hsa.;
bp_group=4;
bp_group_name='Total VHCURES (Excluding Self-Insured)';
subpopulation_num=2;
subpopulation='All-Payer Opioid Use Disorder (OUD) MAT';
run;


/*******************************************************************************
Part 4: % ACG measures Total VHCURES - Other Tx subpopulation 
********************************************************************************/

proc summary data=jdodge.samp NWAY MISSING sum;
class year;
var mm_ave;
where mat_analysis_group='OUD Dx & Other SUD Tx';
output out=othtx_base (drop=_TYPE_ _FREQ_) sum=N;
run;

%macro othtx(rub=,measure_num=);

/*numerators by rub*/
proc summary data=samp NWAY MISSING sum;
class year rub_num;
var mm_ave;
where rub_num=&rub. and mat_analysis_group='OUD Dx & Other SUD Tx';
output out=num_&rub. (drop=_TYPE_ _FREQ_) sum=N;
run;

/*create rate*/
 proc sql;
 create table othtx_&rub. as
 select &measure_num. as measure_num, put(&rub.,rub.) as measure_name, a.year,a.N,a.N/b.N as rate
 from num_&rub. a
 left join othtx_base b on a.year=b.year;
 quit;

%mend;

%othtx(rub=1,measure_num=5);
%othtx(rub=2,measure_num=6);
%othtx(rub=3,measure_num=7);
%othtx(rub=4,measure_num=8);
%othtx(rub=5,measure_num=9);

data part4;
length measure_name bp_group_name subpopulation $100;
set othtx_1 othtx_2 othtx_3 othtx_4 othtx_5;
LCL=.;
UCL=.;
group=&group.;
group_num=&group_num.;
hsa_num=&hsa_num.;
hsa=&hsa.;
bp_group=4;
bp_group_name='Total VHCURES (Excluding Self-Insured)';
subpopulation_num=3;
subpopulation='All-Payer Opioid Use Disorder (OUD) Other Tx';
run;

/*******************************************************************************
Part 5: % ACG measures by attribution group and OUD subpopulation 
********************************************************************************/


%macro matgp(table=,measure_num=,bp_group=,bp_prac=,rub=,mat=,subpop=,out=);

/*denominator*/
proc summary data=samp NWAY MISSING sum;
class year;
var mm_ave;
where bp_prac=&bp_prac. and mat_analysis_group=&mat.;
output out=&table._denom (drop=_TYPE_ _FREQ_) sum=N;
run;

/*numerator*/
proc summary data=samp NWAY MISSING sum;
class year;
var mm_ave;
where rub_num=&rub. and bp_prac=&bp_prac.and mat_analysis_group=&mat.  and rub_num=&rub. ;
output out=&table. (drop=_TYPE_ _FREQ_) sum=N;
run;

/*create rate*/
proc sql;
create table &out. as
select 
&measure_num. as measure_num
,put(&rub.,rub.) as measure_name
, a.year
, a.N
, a.N/b.N as rate
, &bp_group. as bp_group
, put(&bp_prac.,bp.) as bp_group_name
, &subpop. as subpopulation_num
, put(&subpop.,subpop.) as subpopulation
from &table. a 
left join &table._denom b on a.year=b.year  ;
quit;
%mend;

/*PCMH Primary Care Attributed - MAT*/
%matgp(table=pcmhmat1,measure_num=5,bp_group=1,bp_prac=1,rub=1,subpop=2,mat='OUD Dx & MAT',out=pcmhmatf1);/*pcmh attributed,healthy users, mat group*/
%matgp(table=pcmhmat2,measure_num=6,bp_group=1,bp_prac=1,rub=2,subpop=2,mat='OUD Dx & MAT',out=pcmhmatf2);/*pcmh attributed,low risk, mat group*/
%matgp(table=pcmhmat3,measure_num=7,bp_group=1,bp_prac=1,rub=3,subpop=2,mat='OUD Dx & MAT',out=pcmhmatf3);/*pcmh attributed,mod risk, mat group*/
%matgp(table=pcmhmat4,measure_num=8,bp_group=1,bp_prac=1,rub=4,subpop=2,mat='OUD Dx & MAT',out=pcmhmatf4);/*pcmh attributed,high risk, mat group*/
%matgp(table=pcmhmat5,measure_num=9,bp_group=1,bp_prac=1,rub=5,subpop=2,mat='OUD Dx & MAT',out=pcmhmatf5);/*pcmh attributed,very high risk, mat group*/
/*PCMH Primary Care Attributed - Other*/
%matgp(table=pcmhoth1,measure_num=5,bp_group=1,bp_prac=1,rub=1,subpop=3,mat='OUD Dx & Other SUD Tx',out=pcmhothf1);/*pcmh attributed,healthy users, other group*/
%matgp(table=pcmhoth2,measure_num=6,bp_group=1,bp_prac=1,rub=2,subpop=3,mat='OUD Dx & Other SUD Tx',out=pcmhothf2);/*pcmh attributed,low risk, other group*/
%matgp(table=pcmhoth3,measure_num=7,bp_group=1,bp_prac=1,rub=3,subpop=3,mat='OUD Dx & Other SUD Tx',out=pcmhothf3);/*pcmh attributed,mod risk, other group*/
%matgp(table=pcmhoth4,measure_num=8,bp_group=1,bp_prac=1,rub=4,subpop=3,mat='OUD Dx & Other SUD Tx',out=pcmhothf4);/*pcmh attributed,high risk, other group*/
%matgp(table=pcmhoth5,measure_num=9,bp_group=1,bp_prac=1,rub=5,subpop=3,mat='OUD Dx & Other SUD Tx',out=pcmhothf5);/*pcmh attributed,very high risk, other group*/
/*Other Primary Care Attributed - MAT*/
%matgp(table=othmat1,measure_num=5,bp_group=2,bp_prac=0,rub=1,subpop=2,mat='OUD Dx & MAT',out=othmatf1);/*other attributed,healthy users, mat group*/
%matgp(table=othmat2,measure_num=6,bp_group=2,bp_prac=0,rub=2,subpop=2,mat='OUD Dx & MAT',out=othmatf2);/*other attributed,low risk, mat group*/
%matgp(table=othmat3,measure_num=7,bp_group=2,bp_prac=0,rub=3,subpop=2,mat='OUD Dx & MAT',out=othmatf3);/*other attributed,mod risk, mat group*/
%matgp(table=othmat4,measure_num=8,bp_group=2,bp_prac=0,rub=4,subpop=2,mat='OUD Dx & MAT',out=othmatf4);/*other attributed,high risk, mat group*/
%matgp(table=othmat5,measure_num=9,bp_group=2,bp_prac=0,rub=5,subpop=2,mat='OUD Dx & MAT',out=othmatf5);/*other attributed,very high risk, mat group*/
/*Other Primary Care Attributed - Other*/
%matgp(table=othoth1,measure_num=5,bp_group=2,bp_prac=0,rub=1,subpop=3,mat='OUD Dx & Other SUD Tx',out=othothf1);/*other attributed,healthy users, other group*/
%matgp(table=othoth2,measure_num=6,bp_group=2,bp_prac=0,rub=2,subpop=3,mat='OUD Dx & Other SUD Tx',out=othothf2);/*other attributed,low risk, other group*/
%matgp(table=othoth3,measure_num=7,bp_group=2,bp_prac=0,rub=3,subpop=3,mat='OUD Dx & Other SUD Tx',out=othothf3);/*other attributed,mod risk, other group*/
%matgp(table=othoth4,measure_num=8,bp_group=2,bp_prac=0,rub=4,subpop=3,mat='OUD Dx & Other SUD Tx',out=othothf4);/*other attributed,high risk, other group*/
%matgp(table=othoth5,measure_num=9,bp_group=2,bp_prac=0,rub=5,subpop=3,mat='OUD Dx & Other SUD Tx',out=othothf5);/*other attributed,very high risk, other group*/
/*No Primary Care Attributed - MAT*/
%matgp(table=nopcmat1,measure_num=5,bp_group=3,bp_prac=-1,rub=1,subpop=2,mat='OUD Dx & MAT',out=nopcmatf1);/*no pc attributed,healthy users, mat group*/
%matgp(table=nopcmat2,measure_num=6,bp_group=3,bp_prac=-1,rub=2,subpop=2,mat='OUD Dx & MAT',out=nopcmatf2);/*no pc attributed,low risk, mat group*/
%matgp(table=nopcmat3,measure_num=7,bp_group=3,bp_prac=-1,rub=3,subpop=2,mat='OUD Dx & MAT',out=nopcmatf3);/*no pc attributed,mod risk, mat group*/
%matgp(table=nopcmat4,measure_num=8,bp_group=3,bp_prac=-1,rub=4,subpop=2,mat='OUD Dx & MAT',out=nopcmatf4);/*no pc attributed,high risk, mat group*/
%matgp(table=nopcmat5,measure_num=9,bp_group=3,bp_prac=-1,rub=5,subpop=2,mat='OUD Dx & MAT',out=nopcmatf5);/*no pc attributed,very high risk, mat group*/
/*No Primary Care Attributed - Other*/
%matgp(table=nopcoth1,measure_num=5,bp_group=3,bp_prac=-1,rub=1,subpop=3,mat='OUD Dx & Other SUD Tx',out=nopcothf1);/*no pc attributed,healthy users, other group*/
%matgp(table=nopcoth2,measure_num=6,bp_group=3,bp_prac=-1,rub=2,subpop=3,mat='OUD Dx & Other SUD Tx',out=nopcothf2);/*no pc attributed,low risk, other group*/
%matgp(table=nopcoth3,measure_num=7,bp_group=3,bp_prac=-1,rub=3,subpop=3,mat='OUD Dx & Other SUD Tx',out=nopcothf3);/*no pc attributed,mod risk, other group*/
%matgp(table=nopcoth4,measure_num=8,bp_group=3,bp_prac=-1,rub=4,subpop=3,mat='OUD Dx & Other SUD Tx',out=nopcothf4);/*no pc attributed,high risk, other group*/
%matgp(table=nopcoth5,measure_num=9,bp_group=3,bp_prac=-1,rub=5,subpop=3,mat='OUD Dx & Other SUD Tx',out=nopcothf5);/*no pc attributed,very high risk, other group*/

data part5;
length measure_name bp_group_name subpopulation $100;
set pcmhmatf1 pcmhmatf2 pcmhmatf3 pcmhmatf4 pcmhmatf5 pcmhothf1 pcmhothf2 pcmhothf3 pcmhothf4 pcmhothf5
othmatf1 othmatf2 othmatf3 othmatf4 othmatf5 othothf1 othothf2 othothf3 othothf4 othothf5
nopcmatf1 nopcmatf2 nopcmatf3 nopcmatf4 nopcmatf5 nopcothf1 nopcothf2 nopcothf3 nopcothf4 nopcothf5;
LCL=.;
UCL=.;
group=&group.;
group_num=&group_num.;
hsa_num=&hsa_num.;
hsa=&hsa.;

run;


/*******************************************************************************
Part 6: % ACG measures Total VHCURES - WHI subpopulation 
********************************************************************************/

proc summary data=samp NWAY MISSING sum;
class year;
var mm_ave;
where whi=1;
output out=whi_base (drop=_TYPE_ _FREQ_) sum=N;
run;

%macro whi1(rub=,measure_num=);

/*numerators by rub*/
proc summary data=samp NWAY MISSING sum;
class year rub_num;
var mm_ave;
where rub_num=&rub. and whi=1;
output out=num_&rub. (drop=_TYPE_ _FREQ_) sum=N;
run;

/*create rate*/
 proc sql;
 create table whi_&rub. as
 select &measure_num. as measure_num, put(&rub.,rub.) as measure_name, a.year,a.N,a.N/b.N as rate
 from num_&rub. a
 left join whi_base b on a.year=b.year;
 quit;

%mend;

%whi1(rub=1,measure_num=5);
%whi1(rub=2,measure_num=6);
%whi1(rub=3,measure_num=7);
%whi1(rub=4,measure_num=8);
%whi1(rub=5,measure_num=9);

data part6;
length measure_name bp_group_name subpopulation $100;
set whi_1 whi_2 whi_3 whi_4 whi_5;
LCL=.;
UCL=.;
group=&group.;
group_num=&group_num.;
hsa_num=&hsa_num.;
hsa=&hsa.;
bp_group=4;
bp_group_name='Total VHCURES (Excluding Self-Insured)';
subpopulation_num=4;
subpopulation='All-Payer Women Ages 15-44';
run;


/*******************************************************************************
Part 7: % ACG measures by attribution group - WHI subpopulation 
********************************************************************************/

%macro whi(table=,measure_num=,bp_group=,bp_prac=,rub=,subpop=,out=);

/*denominator*/
proc summary data=samp NWAY MISSING sum;
class year;
var mm_ave;
where bp_prac=&bp_prac. and whi=1;
output out=&table._denom (drop=_TYPE_ _FREQ_) sum=N;
run;

/*numerator*/
proc summary data=samp NWAY MISSING sum;
class year;
var mm_ave;
where rub_num=&rub. and bp_prac=&bp_prac.and whi=1  ;
output out=&table. (drop=_TYPE_ _FREQ_) sum=N;
run;

/*create rate*/
proc sql;
create table &out. as
select 
&measure_num. as measure_num
,put(&rub.,rub.) as measure_name
, a.year
, a.N
, a.N/b.N as rate
, &bp_group. as bp_group
, put(&bp_prac.,bp.) as bp_group_name
, &subpop. as subpopulation_num
, put(&subpop.,subpop.) as subpopulation
from &table. a 
left join &table._denom b on a.year=b.year  ;
quit;
%mend;

/*PCMH Primary Care Attributed - WHI*/
%whi(table=pcmhwhi1,measure_num=5,bp_group=1,bp_prac=1,rub=1,subpop=4,out=pcmhwhif1);
%whi(table=pcmhwhi2,measure_num=6,bp_group=1,bp_prac=1,rub=2,subpop=4,out=pcmhwhif2);
%whi(table=pcmhwhi3,measure_num=7,bp_group=1,bp_prac=1,rub=3,subpop=4,out=pcmhwhif3);
%whi(table=pcmhwhi4,measure_num=8,bp_group=1,bp_prac=1,rub=4,subpop=4,out=pcmhwhif4);
%whi(table=pcmhwhi5,measure_num=9,bp_group=1,bp_prac=1,rub=5,subpop=4,out=pcmhwhif5);
/*Other Primary Care Attributed - WHI*/
%whi(table=othwhi1,measure_num=5,bp_group=2,bp_prac=0,rub=1,subpop=4,out=othwhif1);
%whi(table=othwhi2,measure_num=6,bp_group=2,bp_prac=0,rub=2,subpop=4,out=othwhif2);
%whi(table=othwhi3,measure_num=7,bp_group=2,bp_prac=0,rub=3,subpop=4,out=othwhif3);
%whi(table=othwhi4,measure_num=8,bp_group=2,bp_prac=0,rub=4,subpop=4,out=othwhif4);
%whi(table=othwhi5,measure_num=9,bp_group=2,bp_prac=0,rub=5,subpop=4,out=othwhif5);
/*No Primary Care Attributed - WHI*/
%whi(table=nopcwhi1,measure_num=5,bp_group=3,bp_prac=-1,rub=1,subpop=4,out=nopcwhif1);
%whi(table=nopcwhi2,measure_num=6,bp_group=3,bp_prac=-1,rub=2,subpop=4,out=nopcwhif2);
%whi(table=nopcwhi3,measure_num=7,bp_group=3,bp_prac=-1,rub=3,subpop=4,out=nopcwhif3);
%whi(table=nopcwhi4,measure_num=8,bp_group=3,bp_prac=-1,rub=4,subpop=4,out=nopcwhif4);
%whi(table=nopcwhi5,measure_num=9,bp_group=3,bp_prac=-1,rub=5,subpop=4,out=nopcwhif5);

data part7;
length measure_name bp_group_name subpopulation $100;
set pcmhwhif1 pcmhwhif2 pcmhwhif3 pcmhwhif4 pcmhwhif5 
othwhif1 othwhif2 othwhif3 othwhif4 othwhif5
nopcwhif1 nopcwhif2 nopcwhif3 nopcwhif4 nopcwhif5;
LCL=.;
UCL=.;
group=&group.;
group_num=&group_num.;
hsa_num=&hsa_num.;
hsa=&hsa.;
run;


/*******************************************************************************
Part 8: % ACG measures By HSA 
********************************************************************************/

%let group_num=2;
%let group='HSA';

%macro hsa_rub(member_hsa=,hsa_num=);
%macro hsa(rub=,measure_num=);
proc summary data=samp NWAY MISSING sum;
class year;
var mm_ave;
where member_hsa=&member_hsa.;
output out=hsa_base (drop=_TYPE_ _FREQ_) sum=N;
run;

/*numerators by rub*/
proc summary data=samp NWAY MISSING sum;
class year rub_num;
var mm_ave;
where rub_num=&rub. and member_hsa=&member_hsa.;
output out=num_&rub. (drop=_TYPE_ _FREQ_) sum=N;
run;

/*create rate*/
 proc sql;
 create table hsa_&rub. as
 select &measure_num. as measure_num, put(&rub.,rub.) as measure_name, a.year,a.N,a.N/b.N as rate
 ,&hsa_num. as hsa_num
 ,&member_hsa. as hsa
 from num_&rub. a
 left join hsa_base b on a.year=b.year;
 quit;

%mend;

%hsa(rub=1,measure_num=5);
%hsa(rub=2,measure_num=6);
%hsa(rub=3,measure_num=7);
%hsa(rub=4,measure_num=8);
%hsa(rub=5,measure_num=9);

 data hsa_&hsa_num._final;
 set hsa_1 hsa_2 hsa_3 hsa_4 hsa_5;
 run;
 %mend;

 %hsa_rub(member_hsa='Barre',hsa_num=2);
 %hsa_rub(member_hsa='Burlington',hsa_num=3);
 %hsa_rub(member_hsa='Morrisville',hsa_num=4);
 %hsa_rub(member_hsa='Randolph',hsa_num=5);
 %hsa_rub(member_hsa='Newport',hsa_num=6);
 %hsa_rub(member_hsa='St Johnsbury',hsa_num=7);
 %hsa_rub(member_hsa='St Albans',hsa_num=8);
 %hsa_rub(member_hsa='Middlebury',hsa_num=9);
 %hsa_rub(member_hsa='Rutland',hsa_num=10);
 %hsa_rub(member_hsa='Bennington',hsa_num=11);
 %hsa_rub(member_hsa='Springfield',hsa_num=12);
 %hsa_rub(member_hsa='White River Jct',hsa_num=13);
 %hsa_rub(member_hsa='Brattleboro',hsa_num=14);



data part8;
length measure_name bp_group_name subpopulation hsa $100;
set hsa_2_final hsa_3_final hsa_4_final hsa_5_final hsa_6_final hsa_7_final hsa_8_final hsa_9_final hsa_10_final hsa_11_final hsa_12_final hsa_13_final hsa_14_final;
LCL=.;
UCL=.;
group=&group.;
group_num=&group_num.;
bp_group=4;
bp_group_name='Total VHCURES (Excluding Self-Insured)';
subpopulation_num=1;
subpopulation='ALL';
run;

/*merge all pieces in ACG Health Status*/
data acg_final;
retain source_id	PRODUCT_TYPE MEASURE_CATEGORY_NUM MEASURE_CATEGORY MEASURE_TYPE_NUM	MEASURE_TYPE MEASURE_NUM MEASURE_NAME YEAR N RATE 
LCL	UCL	GROUP_NUM GROUP	HSA_NUM	HSA	BP_GROUP BP_GROUP_NAME SUBPOPULATION_NUM SUBPOPULATION ;
length measure_name bp_group_name subpopulation hsa $100;
set part1 part2 part3 part4 part5 part6 part7 part8;
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
run;
/*proc datasets lib=work;*/
/*delete part1 part2 part3 part4 part5 part6 part7 part8 samp;*/
/*run;*/
data yee.acg_final;
set acg_final;
run;

/*check for truncation*/
proc freq data=acg_final;
tables measure_name / missing;
tables bp_group_name / missing;
tables subpopulation / missing;
run;


/*QA*/

/*output for QA 01-03*/
/*data qa1 ;*/
/*retain source_id	PRODUCT_TYPE MEASURE_CATEGORY_NUM MEASURE_CATEGORY MEASURE_TYPE_NUM	MEASURE_TYPE MEASURE_NUM MEASURE_NAME YEAR N RATE */
/*LCL	UCL	GROUP_NUM GROUP	HSA_NUM	HSA	BP_GROUP BP_GROUP_NAME SUBPOPULATION_NUM SUBPOPULATION ;*/
/*length product_type measure_type measure_name bp_group_name subpopulation hsa $100;*/
/*set acg_final demos_final pop_final;*/
/*run;*/
/**/
/*proc freq data=qa1;*/
/*tables product_type measure_name measure_type bp_group_name subpopulation year hsa / missing;*/
/*run;*/

/*Most (60%) of the no primary care attributed group are in the 0 RUB category - no Dx or invalid Dx - this is reflected in output*/
/*proc freq data=samp;*/
/*tables rub / missing;*/
/*where year=2019 and bp_prac=-1;*/
/*run;*/