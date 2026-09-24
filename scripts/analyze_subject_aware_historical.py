import csv, statistics, sys
from pathlib import Path
import matplotlib.pyplot as plt

root = Path(sys.argv[1])
rows = list(csv.DictReader((root / 'subject_aware_measurements.csv').open()))
num = lambda r, k: float(r[k])
plans = ['AggregatePushdown', 'TimestampOnlyBindJoin', 'SubjectAwareBindJoin']
sizes = sorted({int(r['historical_quads']) for r in rows})
def group(n,p): return [r for r in rows if int(r['historical_quads'])==n and r['plan']==p]
def mean(g,k): return statistics.mean(num(r,k) for r in g)
def med(g,k): return statistics.median(num(r,k) for r in g)
def p95(g,k): return sorted(num(r,k) for r in g)[-1 if len(g)==1 else int((len(g)-1)*.95+0.999999)]
def sd(g,k): return statistics.pstdev(num(r,k) for r in g)
with (root/'subject_aware_summary.csv').open('w',newline='') as f:
 w=csv.writer(f); w.writerow('historical_quads plan n historical_storage_mean_ms historical_storage_median_ms historical_storage_stddev_ms historical_storage_p95_ms total_mean_ms total_median_ms total_stddev_ms total_p95_ms mean_storage_records_examined mean_storage_records_matched mean_storage_records_returned mean_operator_output_rows mean_index_entries_examined mean_segments_touched'.split())
 for n in sizes:
  for p in plans:
   g=group(n,p); w.writerow([n,p,len(g),*[f'{x:.6f}' for x in (mean(g,'historical_storage_ms'),med(g,'historical_storage_ms'),sd(g,'historical_storage_ms'),p95(g,'historical_storage_ms'),mean(g,'total_execution_ms'),med(g,'total_execution_ms'),sd(g,'total_execution_ms'),p95(g,'total_execution_ms'),mean(g,'storage_records_examined'),mean(g,'storage_records_matched'),mean(g,'storage_records_returned'),mean(g,'operator_output_rows'),mean(g,'index_entries_examined'),mean(g,'segments_touched'))]])
def reduction(a,b): return 0 if a==0 else 100*(a-b)/a
with (root/'subject_aware_comparison.csv').open('w',newline='') as f, (root/'aggregate_vs_indexed_bind.csv').open('w',newline='') as q:
 w=csv.writer(f); z=csv.writer(q)
 w.writerow('historical_quads timestamp_records_examined indexed_records_examined records_examined_reduction_pct timestamp_records_returned indexed_records_returned timestamp_index_entries_examined indexed_index_entries_examined timestamp_segments_touched indexed_segments_touched timestamp_median_storage_ms indexed_median_storage_ms storage_latency_reduction_pct timestamp_median_total_ms indexed_median_total_ms total_latency_reduction_pct timestamp_p95_total_ms indexed_p95_total_ms timestamp_operator_output_rows indexed_operator_output_rows base_storage_bytes subject_index_bytes index_overhead_pct'.split())
 z.writerow('historical_quads aggregate_records_examined indexed_bind_records_examined aggregate_records_returned indexed_bind_records_returned aggregate_median_storage_ms indexed_bind_median_storage_ms storage_latency_reduction_pct aggregate_median_total_ms indexed_bind_median_total_ms total_latency_reduction_pct aggregate_p95_total_ms indexed_bind_p95_total_ms aggregate_operator_output_rows indexed_bind_operator_output_rows'.split())
 for n in sizes:
  a,t,i=group(n,plans[0]),group(n,plans[1]),group(n,plans[2]); tm,im=med(t,'historical_storage_ms'),med(i,'historical_storage_ms'); tt,it=med(t,'total_execution_ms'),med(i,'total_execution_ms'); am,at=med(a,'historical_storage_ms'),med(a,'total_execution_ms'); base,idx=mean(i,'base_storage_bytes'),mean(i,'subject_index_bytes')
  w.writerow([n,mean(t,'storage_records_examined'),mean(i,'storage_records_examined'),reduction(mean(t,'storage_records_examined'),mean(i,'storage_records_examined')),mean(t,'storage_records_returned'),mean(i,'storage_records_returned'),mean(t,'index_entries_examined'),mean(i,'index_entries_examined'),mean(t,'segments_touched'),mean(i,'segments_touched'),tm,im,reduction(tm,im),tt,it,reduction(tt,it),p95(t,'total_execution_ms'),p95(i,'total_execution_ms'),mean(t,'operator_output_rows'),mean(i,'operator_output_rows'),base,idx,100*idx/base])
  z.writerow([n,mean(a,'storage_records_examined'),mean(i,'storage_records_examined'),mean(a,'storage_records_returned'),mean(i,'storage_records_returned'),am,im,reduction(am,im),at,it,reduction(at,it),p95(a,'total_execution_ms'),p95(i,'total_execution_ms'),mean(a,'operator_output_rows'),mean(i,'operator_output_rows')])
colors={'AggregatePushdown':'#4063a0','TimestampOnlyBindJoin':'#c97a28','SubjectAwareBindJoin':'#567f57'}
def chart(column,title,file):
 plt.figure(figsize=(7,4.2));
 for p in plans: plt.plot(sizes,[mean(group(n,p),column) for n in sizes],marker='o',label=p,color=colors[p])
 plt.xscale('log');plt.xlabel('Historical quads');plt.ylabel(column.replace('_',' '));plt.title(title);plt.grid(True,alpha=.25);plt.legend();plt.tight_layout();plt.savefig(root/file,dpi=180);plt.close()
chart('storage_records_examined','Storage records examined','storage_records_examined.png');chart('storage_records_returned','Storage records returned','storage_records_returned.png');chart('index_entries_examined','Index entries examined','index_entries_examined.png');chart('historical_storage_ms','Historical storage latency','historical_storage_latency.png');chart('total_execution_ms','Total execution latency','total_latency.png')
plt.figure(figsize=(7,4.2)); plt.plot(sizes,[mean(group(n,plans[2]),'subject_index_bytes')/mean(group(n,plans[2]),'base_storage_bytes')*100 for n in sizes],marker='o',color='#567f57');plt.xscale('log');plt.xlabel('Historical quads');plt.ylabel('.sidx overhead (%)');plt.grid(True,alpha=.25);plt.tight_layout();plt.savefig(root/'sidx_storage_overhead.png',dpi=180);plt.close()
for a,b,file in [(plans[1],plans[2],'timestamp_vs_indexed.png'),(plans[0],plans[2],'aggregate_vs_indexed.png')]:
 plt.figure(figsize=(7,4.2));
 for p in [a,b]: plt.plot(sizes,[med(group(n,p),'total_execution_ms') for n in sizes],marker='o',label=p,color=colors[p])
 plt.xscale('log');plt.xlabel('Historical quads');plt.ylabel('Median total execution (ms)');plt.grid(True,alpha=.25);plt.legend();plt.tight_layout();plt.savefig(root/file,dpi=180);plt.close()
