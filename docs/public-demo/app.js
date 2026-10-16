
const $ = id => document.getElementById(id);
function node(tag,text,cls){const n=document.createElement(tag);n.textContent=String(text);if(cls)n.className=cls;return n;}
function rows(items){$('details').replaceChildren(...items.map(([title,body])=>{const r=node('div','','row');r.append(node('strong',title),node('small',body));return r;}));}
function error(e){$('result').textContent=e.message||String(e);$('result').className='status error';$('details').replaceChildren();}
function ready(text){$('result').textContent=text;$('result').className='status';}

const jobs=[{label:'Old routine',priority:0,start_ms:0,delay_ms:0},{label:'New priority',priority:5,start_ms:1000,delay_ms:0},{label:'Routine A',priority:0,start_ms:1000,delay_ms:0},{label:'Routine B',priority:0,start_ms:1000,delay_ms:0},{label:'Delayed priority',priority:9,start_ms:1000,delay_ms:4000}];
function input(){return {jobs,order:$('order').value,aging:Number($('aging').value),now_ms:Number($('clock').value)*1000};}
function render(d){ready(d.order.length+' jobs ready; '+d.waiting+' waiting');$('tiles').replaceChildren(...d.order.map((s,i)=>node('div',(i+1)+'. '+s,'tile')));rows([['Next delivery',d.order[0]||'Nothing is visible yet'],['Delay rule','Delayed priority becomes visible at 5 seconds and starts aging then.'],['Ordering','Priority minus aging rate times visibility time, then arrival order for ties.']]);}
$('reset').onclick=()=>{HTMLFormElement.prototype.reset.call(document.querySelector('form'));run();};

let worker, sequence=0, pending=new Map();
const boot=new Promise((resolve,reject)=>{
 worker=new Worker('./worker.js');
 const timer=setTimeout(()=>reject(new Error('Engine did not start. Reload to retry.')),20000);
 worker.onerror=()=>{clearTimeout(timer);reject(new Error('Engine failed to load. Reload to retry.'));for(const p of pending.values())p.reject(new Error('Engine stopped. Reload to retry.'));pending.clear();};
 worker.onmessage=({data})=>{if(data.ready){clearTimeout(timer);resolve();return;}if(data.bootError){clearTimeout(timer);reject(new Error(data.bootError));return;}const p=pending.get(data.id);if(p){pending.delete(data.id);data.error?p.reject(new Error(data.error)):p.resolve(data.value);}};
});
async function evaluate(input){await boot;const id=++sequence;return new Promise((resolve,reject)=>{pending.set(id,{resolve,reject});worker.postMessage({id,input});});}
async function run(){if($('run').disabled)return;$('run').disabled=true;try{render(await evaluate(input()));}catch(e){error(e);}finally{$('run').disabled=false;}}
document.querySelector('form').addEventListener('submit',e=>{e.preventDefault();run();});
boot.then(()=>{ $('run').disabled=false; run(); }).catch(error);
