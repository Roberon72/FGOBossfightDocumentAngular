var tw=Object.defineProperty,nw=Object.defineProperties;var rw=Object.getOwnPropertyDescriptors;var Ra=Object.getOwnPropertySymbols;var dg=Object.prototype.hasOwnProperty,ug=Object.prototype.propertyIsEnumerable;var lg=(n,t,e)=>t in n?tw(n,t,{enumerable:!0,configurable:!0,writable:!0,value:e}):n[t]=e,w=(n,t)=>{for(var e in t||={})dg.call(t,e)&&lg(n,e,t[e]);if(Ra)for(var e of Ra(t))ug.call(t,e)&&lg(n,e,t[e]);return n},W=(n,t)=>nw(n,rw(t));var fg=(n,t)=>{var e={};for(var r in n)dg.call(n,r)&&t.indexOf(r)<0&&(e[r]=n[r]);if(n!=null&&Ra)for(var r of Ra(n))t.indexOf(r)<0&&ug.call(n,r)&&(e[r]=n[r]);return e};var ht=null,Oa=!1,eu=1,iw=null,We=Symbol("SIGNAL");function N(n){let t=ht;return ht=n,t}function Na(){return ht}var gr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function br(n){if(Oa)throw new Error("");if(ht===null)return;ht.consumerOnSignalRead(n);let t=ht.producersTail;if(t!==void 0&&t.producer===n)return;let e,r=ht.recomputing;if(r&&(e=t!==void 0?t.nextProducer:ht.producers,e!==void 0&&e.producer===n)){ht.producersTail=e,e.lastReadVersion=n.version;return}let i=n.consumersTail;if(i!==void 0&&i.consumer===ht&&(!r||sw(i,ht)))return;let o=Xi(ht),s={producer:n,consumer:ht,nextProducer:e,prevConsumer:i,lastReadVersion:n.version,nextConsumer:void 0};ht.producersTail=s,t!==void 0?t.nextProducer=s:ht.producers=s,o&&gg(n,s)}function mg(){eu++}function Wr(n){if(!(Xi(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===eu)){if(!n.producerMustRecompute(n)&&!Qi(n)){Ki(n);return}n.producerRecomputeValue(n),Ki(n)}}function tu(n){if(n.consumers===void 0)return;let t=Oa;Oa=!0;try{for(let e=n.consumers;e!==void 0;e=e.nextConsumer){let r=e.consumer;r.dirty||ow(r)}}finally{Oa=t}}function nu(){return ht?.consumerAllowSignalWrites!==!1}function ow(n){n.dirty=!0,tu(n),n.consumerMarkedDirty?.(n)}function Ki(n){n.dirty=!1,n.lastCleanEpoch=eu}function Yn(n){return n&&hg(n),N(n)}function hg(n){n.producersTail=void 0,n.recomputing=!0}function yr(n,t){N(t),n&&pg(n)}function pg(n){n.recomputing=!1;let t=n.producersTail,e=t!==void 0?t.nextProducer:n.producers;if(e!==void 0){if(Xi(n))do e=ru(e);while(e!==void 0);t!==void 0?t.nextProducer=void 0:n.producers=void 0}}function Qi(n){for(let t=n.producers;t!==void 0;t=t.nextProducer){let e=t.producer,r=t.lastReadVersion;if(r!==e.version||(Wr(e),r!==e.version))return!0}return!1}function _r(n){if(Xi(n)){let t=n.producers;for(;t!==void 0;)t=ru(t)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function gg(n,t){let e=n.consumersTail,r=Xi(n);if(e!==void 0?(t.nextConsumer=e.nextConsumer,e.nextConsumer=t):(t.nextConsumer=void 0,n.consumers=t),t.prevConsumer=e,n.consumersTail=t,!r)for(let i=n.producers;i!==void 0;i=i.nextProducer)gg(i.producer,i)}function ru(n){let t=n.producer,e=n.nextProducer,r=n.nextConsumer,i=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,r!==void 0?r.prevConsumer=i:t.consumersTail=i,i!==void 0)i.nextConsumer=r;else if(t.consumers=r,!Xi(t)){let o=t.producers;for(;o!==void 0;)o=ru(o)}return e}function Xi(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function Yo(n){iw?.(n)}function sw(n,t){let e=t.producersTail;if(e!==void 0){let r=t.producers;do{if(r===n)return!0;if(r===e)break;r=r.nextProducer}while(r!==void 0)}return!1}function Zo(n,t){return Object.is(n,t)}function Ko(n,t){let e=Object.create(aw);e.computation=n,t!==void 0&&(e.equal=t);let r=()=>{if(Wr(e),br(e),e.value===Tn)throw e.error;return e.value};return r[We]=e,Yo(e),r}var $r=Symbol("UNSET"),Gr=Symbol("COMPUTING"),Tn=Symbol("ERRORED"),aw=W(w({},gr),{value:$r,dirty:!0,error:null,equal:Zo,kind:"computed",producerMustRecompute(n){return n.value===$r||n.value===Gr},producerRecomputeValue(n){if(n.value===Gr)throw new Error("");let t=n.value;n.value=Gr;let e=Yn(n),r,i=!1;try{r=n.computation(),N(null),i=t!==$r&&t!==Tn&&r!==Tn&&n.equal(t,r)}catch(o){r=Tn,n.error=o}finally{yr(n,e)}if(i){n.value=t;return}n.value=r,n.version++}});function cw(){throw new Error}var bg=cw;function yg(n){bg(n)}function iu(n){bg=n}var lw=null;function ou(n,t){let e=Object.create(Qo);e.value=n,t!==void 0&&(e.equal=t);let r=()=>_g(e);return r[We]=e,Yo(e),[r,s=>qr(e,s),s=>Pa(e,s)]}function _g(n){return br(n),n.value}function qr(n,t){nu()||yg(n),n.equal(n.value,t)||(n.value=t,dw(n))}function Pa(n,t){nu()||yg(n),qr(n,t(n.value))}var Qo=W(w({},gr),{equal:Zo,value:void 0,kind:"signal"});function dw(n){n.version++,mg(),tu(n),lw?.(n)}var su=W(w({},gr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function au(n){if(n.dirty=!1,n.version>0&&!Qi(n))return;n.version++;let t=Yn(n);try{n.cleanup(),n.fn()}finally{yr(n,t)}}function se(n){return typeof n=="function"}function Fa(n){let e=n(r=>{Error.call(r),r.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var La=Fa(n=>function(e){n(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((r,i)=>`${i+1}) ${r.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Yr(n,t){if(n){let e=n.indexOf(t);0<=e&&n.splice(e,1)}}var ye=class n{constructor(t){this.initialTeardown=t,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let t;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:r}=this;if(se(r))try{r()}catch(o){t=o instanceof La?o.errors:[o]}let{_finalizers:i}=this;if(i){this._finalizers=null;for(let o of i)try{vg(o)}catch(s){t=t??[],s instanceof La?t=[...t,...s.errors]:t.push(s)}}if(t)throw new La(t)}}add(t){var e;if(t&&t!==this)if(this.closed)vg(t);else{if(t instanceof n){if(t.closed||t._hasParent(this))return;t._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(t)}}_hasParent(t){let{_parentage:e}=this;return e===t||Array.isArray(e)&&e.includes(t)}_addParent(t){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(t),e):e?[e,t]:t}_removeParent(t){let{_parentage:e}=this;e===t?this._parentage=null:Array.isArray(e)&&Yr(e,t)}remove(t){let{_finalizers:e}=this;e&&Yr(e,t),t instanceof n&&t._removeParent(this)}};ye.EMPTY=(()=>{let n=new ye;return n.closed=!0,n})();var cu=ye.EMPTY;function Ba(n){return n instanceof ye||n&&"closed"in n&&se(n.remove)&&se(n.add)&&se(n.unsubscribe)}function vg(n){se(n)?n():n.unsubscribe()}var on={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Ji={setTimeout(n,t,...e){let{delegate:r}=Ji;return r?.setTimeout?r.setTimeout(n,t,...e):setTimeout(n,t,...e)},clearTimeout(n){let{delegate:t}=Ji;return(t?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Va(n){Ji.setTimeout(()=>{let{onUnhandledError:t}=on;if(t)t(n);else throw n})}function Xo(){}var xg=lu("C",void 0,void 0);function Dg(n){return lu("E",void 0,n)}function wg(n){return lu("N",n,void 0)}function lu(n,t,e){return{kind:n,value:t,error:e}}var Zr=null;function eo(n){if(on.useDeprecatedSynchronousErrorHandling){let t=!Zr;if(t&&(Zr={errorThrown:!1,error:null}),n(),t){let{errorThrown:e,error:r}=Zr;if(Zr=null,e)throw r}}else n()}function Cg(n){on.useDeprecatedSynchronousErrorHandling&&Zr&&(Zr.errorThrown=!0,Zr.error=n)}var Kr=class extends ye{constructor(t){super(),this.isStopped=!1,t?(this.destination=t,Ba(t)&&t.add(this)):this.destination=mw}static create(t,e,r){return new Zn(t,e,r)}next(t){this.isStopped?uu(wg(t),this):this._next(t)}error(t){this.isStopped?uu(Dg(t),this):(this.isStopped=!0,this._error(t))}complete(){this.isStopped?uu(xg,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(t){this.destination.next(t)}_error(t){try{this.destination.error(t)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},uw=Function.prototype.bind;function du(n,t){return uw.call(n,t)}var fu=class{constructor(t){this.partialObserver=t}next(t){let{partialObserver:e}=this;if(e.next)try{e.next(t)}catch(r){ja(r)}}error(t){let{partialObserver:e}=this;if(e.error)try{e.error(t)}catch(r){ja(r)}else ja(t)}complete(){let{partialObserver:t}=this;if(t.complete)try{t.complete()}catch(e){ja(e)}}},Zn=class extends Kr{constructor(t,e,r){super();let i;if(se(t)||!t)i={next:t??void 0,error:e??void 0,complete:r??void 0};else{let o;this&&on.useDeprecatedNextContext?(o=Object.create(t),o.unsubscribe=()=>this.unsubscribe(),i={next:t.next&&du(t.next,o),error:t.error&&du(t.error,o),complete:t.complete&&du(t.complete,o)}):i=t}this.destination=new fu(i)}};function ja(n){on.useDeprecatedSynchronousErrorHandling?Cg(n):Va(n)}function fw(n){throw n}function uu(n,t){let{onStoppedNotification:e}=on;e&&Ji.setTimeout(()=>e(n,t))}var mw={closed:!0,next:Xo,error:fw,complete:Xo};var to=typeof Symbol=="function"&&Symbol.observable||"@@observable";function sn(n){return n}function Eg(n){return n.length===0?sn:n.length===1?n[0]:function(e){return n.reduce((r,i)=>i(r),e)}}var le=(()=>{class n{constructor(e){e&&(this._subscribe=e)}lift(e){let r=new n;return r.source=this,r.operator=e,r}subscribe(e,r,i){let o=pw(e)?e:new Zn(e,r,i);return eo(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(r){e.error(r)}}forEach(e,r){return r=kg(r),new r((i,o)=>{let s=new Zn({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:i});this.subscribe(s)})}_subscribe(e){var r;return(r=this.source)===null||r===void 0?void 0:r.subscribe(e)}[to](){return this}pipe(...e){return Eg(e)(this)}toPromise(e){return e=kg(e),new e((r,i)=>{let o;this.subscribe(s=>o=s,s=>i(s),()=>r(o))})}}return n.create=t=>new n(t),n})();function kg(n){var t;return(t=n??on.Promise)!==null&&t!==void 0?t:Promise}function hw(n){return n&&se(n.next)&&se(n.error)&&se(n.complete)}function pw(n){return n&&n instanceof Kr||hw(n)&&Ba(n)}function gw(n){return se(n?.lift)}function he(n){return t=>{if(gw(t))return t.lift(function(e){try{return n(e,this)}catch(r){this.error(r)}});throw new TypeError("Unable to lift unknown Observable type")}}function _e(n,t,e,r,i){return new mu(n,t,e,r,i)}var mu=class extends Kr{constructor(t,e,r,i,o,s){super(t),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){t.error(c)}}:super._next,this._error=i?function(a){try{i(a)}catch(c){t.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=r?function(){try{r()}catch(a){t.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var t;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((t=this.onFinalize)===null||t===void 0||t.call(this))}}};var Ig=Fa(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var E=(()=>{class n extends le{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let r=new Ha(this,this);return r.operator=e,r}_throwIfClosed(){if(this.closed)throw new Ig}next(e){eo(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let r of this.currentObservers)r.next(e)}})}error(e){eo(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:r}=this;for(;r.length;)r.shift().error(e)}})}complete(){eo(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:r,isStopped:i,observers:o}=this;return r||i?cu:(this.currentObservers=null,o.push(e),new ye(()=>{this.currentObservers=null,Yr(o,e)}))}_checkFinalizedStatuses(e){let{hasError:r,thrownError:i,isStopped:o}=this;r?e.error(i):o&&e.complete()}asObservable(){let e=new le;return e.source=this,e}}return n.create=(t,e)=>new Ha(t,e),n})(),Ha=class extends E{constructor(t,e){super(),this.destination=t,this.source=e}next(t){var e,r;(r=(e=this.destination)===null||e===void 0?void 0:e.next)===null||r===void 0||r.call(e,t)}error(t){var e,r;(r=(e=this.destination)===null||e===void 0?void 0:e.error)===null||r===void 0||r.call(e,t)}complete(){var t,e;(e=(t=this.destination)===null||t===void 0?void 0:t.complete)===null||e===void 0||e.call(t)}_subscribe(t){var e,r;return(r=(e=this.source)===null||e===void 0?void 0:e.subscribe(t))!==null&&r!==void 0?r:cu}};var Qr=class extends E{constructor(t){super(),this._value=t}get value(){return this.getValue()}_subscribe(t){let e=super._subscribe(t);return!e.closed&&t.next(this._value),e}getValue(){let{hasError:t,thrownError:e,_value:r}=this;if(t)throw e;return this._throwIfClosed(),r}next(t){super.next(this._value=t)}};var Jo={now(){return(Jo.delegate||Date).now()},delegate:void 0};var vr=class extends E{constructor(t=1/0,e=1/0,r=Jo){super(),this._bufferSize=t,this._windowTime=e,this._timestampProvider=r,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,t),this._windowTime=Math.max(1,e)}next(t){let{isStopped:e,_buffer:r,_infiniteTimeWindow:i,_timestampProvider:o,_windowTime:s}=this;e||(r.push(t),!i&&r.push(o.now()+s)),this._trimBuffer(),super.next(t)}_subscribe(t){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(t),{_infiniteTimeWindow:r,_buffer:i}=this,o=i.slice();for(let s=0;s<o.length&&!t.closed;s+=r?1:2)t.next(o[s]);return this._checkFinalizedStatuses(t),e}_trimBuffer(){let{_bufferSize:t,_timestampProvider:e,_buffer:r,_infiniteTimeWindow:i}=this,o=(i?1:2)*t;if(t<1/0&&o<r.length&&r.splice(0,r.length-o),!i){let s=e.now(),a=0;for(let c=1;c<r.length&&r[c]<=s;c+=2)a=c;a&&r.splice(0,a+1)}}};var za=class extends ye{constructor(t,e){super()}schedule(t,e=0){return this}};var es={setInterval(n,t,...e){let{delegate:r}=es;return r?.setInterval?r.setInterval(n,t,...e):setInterval(n,t,...e)},clearInterval(n){let{delegate:t}=es;return(t?.clearInterval||clearInterval)(n)},delegate:void 0};var Ua=class extends za{constructor(t,e){super(t,e),this.scheduler=t,this.work=e,this.pending=!1}schedule(t,e=0){var r;if(this.closed)return this;this.state=t;let i=this.id,o=this.scheduler;return i!=null&&(this.id=this.recycleAsyncId(o,i,e)),this.pending=!0,this.delay=e,this.id=(r=this.id)!==null&&r!==void 0?r:this.requestAsyncId(o,this.id,e),this}requestAsyncId(t,e,r=0){return es.setInterval(t.flush.bind(t,this),r)}recycleAsyncId(t,e,r=0){if(r!=null&&this.delay===r&&this.pending===!1)return e;e!=null&&es.clearInterval(e)}execute(t,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let r=this._execute(t,e);if(r)return r;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(t,e){let r=!1,i;try{this.work(t)}catch(o){r=!0,i=o||new Error("Scheduled action threw falsy error")}if(r)return this.unsubscribe(),i}unsubscribe(){if(!this.closed){let{id:t,scheduler:e}=this,{actions:r}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Yr(r,this),t!=null&&(this.id=this.recycleAsyncId(e,t,null)),this.delay=null,super.unsubscribe()}}};var no=class n{constructor(t,e=n.now){this.schedulerActionCtor=t,this.now=e}schedule(t,e=0,r){return new this.schedulerActionCtor(this,t).schedule(r,e)}};no.now=Jo.now;var $a=class extends no{constructor(t,e=no.now){super(t,e),this.actions=[],this._active=!1}flush(t){let{actions:e}=this;if(this._active){e.push(t);return}let r;this._active=!0;do if(r=t.execute(t.state,t.delay))break;while(t=e.shift());if(this._active=!1,r){for(;t=e.shift();)t.unsubscribe();throw r}}};var ts=new $a(Ua),Sg=ts;var Xr=new le(n=>n.complete());function Ga(n){return n&&se(n.schedule)}function hu(n){return n[n.length-1]}function Wa(n){return se(hu(n))?n.pop():void 0}function Mn(n){return Ga(hu(n))?n.pop():void 0}function Tg(n,t){return typeof hu(n)=="number"?n.pop():t}function Ag(n,t,e,r){function i(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(d){try{l(r.next(d))}catch(u){s(u)}}function c(d){try{l(r.throw(d))}catch(u){s(u)}}function l(d){d.done?o(d.value):i(d.value).then(a,c)}l((r=r.apply(n,t||[])).next())})}function Mg(n){var t=typeof Symbol=="function"&&Symbol.iterator,e=t&&n[t],r=0;if(e)return e.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&r>=n.length&&(n=void 0),{value:n&&n[r++],done:!n}}};throw new TypeError(t?"Object is not iterable.":"Symbol.iterator is not defined.")}function Jr(n){return this instanceof Jr?(this.v=n,this):new Jr(n)}function Rg(n,t,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var r=e.apply(n,t||[]),i,o=[];return i=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),i[Symbol.asyncIterator]=function(){return this},i;function s(h){return function(p){return Promise.resolve(p).then(h,u)}}function a(h,p){r[h]&&(i[h]=function(g){return new Promise(function(y,D){o.push([h,g,y,D])>1||c(h,g)})},p&&(i[h]=p(i[h])))}function c(h,p){try{l(r[h](p))}catch(g){m(o[0][3],g)}}function l(h){h.value instanceof Jr?Promise.resolve(h.value.v).then(d,u):m(o[0][2],h)}function d(h){c("next",h)}function u(h){c("throw",h)}function m(h,p){h(p),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Og(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var t=n[Symbol.asyncIterator],e;return t?t.call(n):(n=typeof Mg=="function"?Mg(n):n[Symbol.iterator](),e={},r("next"),r("throw"),r("return"),e[Symbol.asyncIterator]=function(){return this},e);function r(o){e[o]=n[o]&&function(s){return new Promise(function(a,c){s=n[o](s),i(a,c,s.done,s.value)})}}function i(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var qa=n=>n&&typeof n.length=="number"&&typeof n!="function";function Ya(n){return se(n?.then)}function Za(n){return se(n[to])}function Ka(n){return Symbol.asyncIterator&&se(n?.[Symbol.asyncIterator])}function Qa(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function bw(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Xa=bw();function Ja(n){return se(n?.[Xa])}function ec(n){return Rg(this,arguments,function*(){let e=n.getReader();try{for(;;){let{value:r,done:i}=yield Jr(e.read());if(i)return yield Jr(void 0);yield yield Jr(r)}}finally{e.releaseLock()}})}function tc(n){return se(n?.getReader)}function Re(n){if(n instanceof le)return n;if(n!=null){if(Za(n))return yw(n);if(qa(n))return _w(n);if(Ya(n))return vw(n);if(Ka(n))return Ng(n);if(Ja(n))return xw(n);if(tc(n))return Dw(n)}throw Qa(n)}function yw(n){return new le(t=>{let e=n[to]();if(se(e.subscribe))return e.subscribe(t);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function _w(n){return new le(t=>{for(let e=0;e<n.length&&!t.closed;e++)t.next(n[e]);t.complete()})}function vw(n){return new le(t=>{n.then(e=>{t.closed||(t.next(e),t.complete())},e=>t.error(e)).then(null,Va)})}function xw(n){return new le(t=>{for(let e of n)if(t.next(e),t.closed)return;t.complete()})}function Ng(n){return new le(t=>{ww(n,t).catch(e=>t.error(e))})}function Dw(n){return Ng(ec(n))}function ww(n,t){var e,r,i,o;return Ag(this,void 0,void 0,function*(){try{for(e=Og(n);r=yield e.next(),!r.done;){let s=r.value;if(t.next(s),t.closed)return}}catch(s){i={error:s}}finally{try{r&&!r.done&&(o=e.return)&&(yield o.call(e))}finally{if(i)throw i.error}}t.complete()})}function It(n,t,e,r=0,i=!1){let o=t.schedule(function(){e(),i?n.add(this.schedule(null,r)):this.unsubscribe()},r);if(n.add(o),!i)return o}function nc(n,t=0){return he((e,r)=>{e.subscribe(_e(r,i=>It(r,n,()=>r.next(i),t),()=>It(r,n,()=>r.complete(),t),i=>It(r,n,()=>r.error(i),t)))})}function rc(n,t=0){return he((e,r)=>{r.add(n.schedule(()=>e.subscribe(r),t))})}function Pg(n,t){return Re(n).pipe(rc(t),nc(t))}function Fg(n,t){return Re(n).pipe(rc(t),nc(t))}function Lg(n,t){return new le(e=>{let r=0;return t.schedule(function(){r===n.length?e.complete():(e.next(n[r++]),e.closed||this.schedule())})})}function Bg(n,t){return new le(e=>{let r;return It(e,t,()=>{r=n[Xa](),It(e,t,()=>{let i,o;try{({value:i,done:o}=r.next())}catch(s){e.error(s);return}o?e.complete():e.next(i)},0,!0)}),()=>se(r?.return)&&r.return()})}function ic(n,t){if(!n)throw new Error("Iterable cannot be null");return new le(e=>{It(e,t,()=>{let r=n[Symbol.asyncIterator]();It(e,t,()=>{r.next().then(i=>{i.done?e.complete():e.next(i.value)})},0,!0)})})}function Vg(n,t){return ic(ec(n),t)}function jg(n,t){if(n!=null){if(Za(n))return Pg(n,t);if(qa(n))return Lg(n,t);if(Ya(n))return Fg(n,t);if(Ka(n))return ic(n,t);if(Ja(n))return Bg(n,t);if(tc(n))return Vg(n,t)}throw Qa(n)}function zt(n,t){return t?jg(n,t):Re(n)}function An(...n){let t=Mn(n);return zt(n,t)}function Hg(n){return n instanceof Date&&!isNaN(n)}function Le(n,t){return he((e,r)=>{let i=0;e.subscribe(_e(r,o=>{r.next(n.call(t,o,i++))}))})}var{isArray:Cw}=Array;function Ew(n,t){return Cw(t)?n(...t):n(t)}function oc(n){return Le(t=>Ew(n,t))}var{isArray:kw}=Array,{getPrototypeOf:Iw,prototype:Sw,keys:Tw}=Object;function sc(n){if(n.length===1){let t=n[0];if(kw(t))return{args:t,keys:null};if(Mw(t)){let e=Tw(t);return{args:e.map(r=>t[r]),keys:e}}}return{args:n,keys:null}}function Mw(n){return n&&typeof n=="object"&&Iw(n)===Sw}function ac(n,t){return n.reduce((e,r,i)=>(e[r]=t[i],e),{})}function pu(...n){let t=Mn(n),e=Wa(n),{args:r,keys:i}=sc(n);if(r.length===0)return zt([],t);let o=new le(Aw(r,t,i?s=>ac(i,s):sn));return e?o.pipe(oc(e)):o}function Aw(n,t,e=sn){return r=>{zg(t,()=>{let{length:i}=n,o=new Array(i),s=i,a=i;for(let c=0;c<i;c++)zg(t,()=>{let l=zt(n[c],t),d=!1;l.subscribe(_e(r,u=>{o[c]=u,d||(d=!0,a--),a||r.next(e(o.slice()))},()=>{--s||r.complete()}))},r)},r)}}function zg(n,t,e){n?It(e,n,t):t()}function Ug(n,t,e,r,i,o,s,a){let c=[],l=0,d=0,u=!1,m=()=>{u&&!c.length&&!l&&t.complete()},h=g=>l<r?p(g):c.push(g),p=g=>{o&&t.next(g),l++;let y=!1;Re(e(g,d++)).subscribe(_e(t,D=>{i?.(D),o?h(D):t.next(D)},()=>{y=!0},void 0,()=>{if(y)try{for(l--;c.length&&l<r;){let D=c.shift();s?It(t,s,()=>p(D)):p(D)}m()}catch(D){t.error(D)}}))};return n.subscribe(_e(t,h,()=>{u=!0,m()})),()=>{a?.()}}function ro(n,t,e=1/0){return se(t)?ro((r,i)=>Le((o,s)=>t(r,o,i,s))(Re(n(r,i))),e):(typeof t=="number"&&(e=t),he((r,i)=>Ug(r,i,n,e)))}function cc(n=1/0){return ro(sn,n)}function $g(){return cc(1)}function io(...n){return $g()(zt(n,Mn(n)))}function ei(n){return new le(t=>{Re(n()).subscribe(t)})}function gu(...n){let t=Wa(n),{args:e,keys:r}=sc(n),i=new le(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let u=!1;Re(e[d]).subscribe(_e(o,m=>{u||(u=!0,l--),a[d]=m},()=>c--,void 0,()=>{(!c||!u)&&(l||o.next(r?ac(r,a):a),o.complete())}))}});return t?i.pipe(oc(t)):i}function Gg(n=0,t,e=Sg){let r=-1;return t!=null&&(Ga(t)?e=t:r=t),new le(i=>{let o=Hg(n)?+n-e.now():n;o<0&&(o=0);let s=0;return e.schedule(function(){i.closed||(i.next(s++),0<=r?this.schedule(void 0,r):i.complete())},o)})}function an(...n){let t=Mn(n),e=Tg(n,1/0),r=n;return r.length?r.length===1?Re(r[0]):cc(e)(zt(r,t)):Xr}function Ie(n,t){return he((e,r)=>{let i=0;e.subscribe(_e(r,o=>n.call(t,o,i++)&&r.next(o)))})}function Wg(n){return he((t,e)=>{let r=!1,i=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,r){r=!1;let l=i;i=null,e.next(l)}s&&e.complete()},c=()=>{o=null,s&&e.complete()};t.subscribe(_e(e,l=>{r=!0,i=l,o||Re(n(l)).subscribe(o=_e(e,a,c))},()=>{s=!0,(!r||!o||o.closed)&&e.complete()}))})}function lc(n,t=ts){return Wg(()=>Gg(n,t))}function bu(n,t){return se(t)?ro(n,t,1):ro(n,1)}function ns(n,t=ts){return he((e,r)=>{let i=null,o=null,s=null,a=()=>{if(i){i.unsubscribe(),i=null;let l=o;o=null,r.next(l)}};function c(){let l=s+n,d=t.now();if(d<l){i=this.schedule(void 0,l-d),r.add(i);return}a()}e.subscribe(_e(r,l=>{o=l,s=t.now(),i||(i=t.schedule(c,n),r.add(i))},()=>{a(),r.complete()},void 0,()=>{o=i=null}))})}function Mt(n){return n<=0?()=>Xr:he((t,e)=>{let r=0;t.subscribe(_e(e,i=>{++r<=n&&(e.next(i),n<=r&&e.complete())}))})}function dc(n,t=sn){return n=n??Rw,he((e,r)=>{let i,o=!0;e.subscribe(_e(r,s=>{let a=t(s);(o||!n(i,a))&&(o=!1,i=a,r.next(s))}))})}function Rw(n,t){return n===t}function yu(n){return he((t,e)=>{try{t.subscribe(e)}finally{e.add(n)}})}function uc(){return he((n,t)=>{let e,r=!1;n.subscribe(_e(t,i=>{let o=e;e=i,r&&t.next([o,i]),r=!0}))})}function qg(n={}){let{connector:t=()=>new E,resetOnError:e=!0,resetOnComplete:r=!0,resetOnRefCountZero:i=!0}=n;return o=>{let s,a,c,l=0,d=!1,u=!1,m=()=>{a?.unsubscribe(),a=void 0},h=()=>{m(),s=c=void 0,d=u=!1},p=()=>{let g=s;h(),g?.unsubscribe()};return he((g,y)=>{l++,!u&&!d&&m();let D=c=c??t();y.add(()=>{l--,l===0&&!u&&!d&&(a=_u(p,i))}),D.subscribe(y),!s&&l>0&&(s=new Zn({next:R=>D.next(R),error:R=>{u=!0,m(),a=_u(h,e,R),D.error(R)},complete:()=>{d=!0,m(),a=_u(h,r),D.complete()}}),Re(g).subscribe(s))})(o)}}function _u(n,t,...e){if(t===!0){n();return}if(t===!1)return;let r=new Zn({next:()=>{r.unsubscribe(),n()}});return Re(t(...e)).subscribe(r)}function fc(n,t,e){let r,i=!1;return n&&typeof n=="object"?{bufferSize:r=1/0,windowTime:t=1/0,refCount:i=!1,scheduler:e}=n:r=n??1/0,qg({connector:()=>new vr(r,t,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:i})}function rs(n){return Ie((t,e)=>n<=e)}function pt(...n){let t=Mn(n);return he((e,r)=>{(t?io(n,e,t):io(n,e)).subscribe(r)})}function ti(n,t){return he((e,r)=>{let i=null,o=0,s=!1,a=()=>s&&!i&&r.complete();e.subscribe(_e(r,c=>{i?.unsubscribe();let l=0,d=o++;Re(n(c,d)).subscribe(i=_e(r,u=>r.next(t?t(c,u,d,l++):u),()=>{i=null,a()}))},()=>{s=!0,a()}))})}function st(n){return he((t,e)=>{Re(n).subscribe(_e(e,()=>e.complete(),Xo)),!e.closed&&t.subscribe(e)})}function vu(n,t=!1){return he((e,r)=>{let i=0;e.subscribe(_e(r,o=>{let s=n(o,i++);(s||t)&&r.next(o),!s&&r.complete()}))})}function xu(n,t,e){let r=se(n)||t||e?{next:n,error:t,complete:e}:n;return r?he((i,o)=>{var s;(s=r.subscribe)===null||s===void 0||s.call(r);let a=!0;i.subscribe(_e(o,c=>{var l;(l=r.next)===null||l===void 0||l.call(r,c),o.next(c)},()=>{var c;a=!1,(c=r.complete)===null||c===void 0||c.call(r),o.complete()},c=>{var l;a=!1,(l=r.error)===null||l===void 0||l.call(r,c),o.error(c)},()=>{var c,l;a&&((c=r.unsubscribe)===null||c===void 0||c.call(r)),(l=r.finalize)===null||l===void 0||l.call(r)}))}):sn}var Du;function mc(){return Du}function Rn(n){let t=Du;return Du=n,t}var Yg=Symbol("NotFound");function oo(n){return n===Yg||n?.name==="\u0275NotFound"}function wu(n,t,e){let r=Object.create(Ow);r.source=n,r.computation=t,e!=null&&(r.equal=e);let o=()=>{if(Wr(r),br(r),r.value===Tn)throw r.error;return r.value};return o[We]=r,Yo(r),o}function Zg(n,t){Wr(n),qr(n,t),Ki(n)}function Kg(n,t){if(Wr(n),n.value===Tn)throw n.error;Pa(n,t),Ki(n)}var Ow=W(w({},gr),{value:$r,dirty:!0,error:null,equal:Zo,kind:"linkedSignal",producerMustRecompute(n){return n.value===$r||n.value===Gr},producerRecomputeValue(n){if(n.value===Gr)throw new Error("");let t=n.value;n.value=Gr;let e=Yn(n),r,i=!1;try{let o=n.source(),s=t!==$r&&t!==Tn,a=s?{source:n.sourceValue,value:t}:void 0;r=n.computation(o,a),n.sourceValue=o,N(null),i=s&&r!==Tn&&n.equal(t,r)}catch(o){r=Tn,n.error=o}finally{yr(n,e)}if(i){n.value=t;return}n.value=r,n.version++}});function Qg(n){let t=N(null);try{return n()}finally{N(t)}}var vc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",M=class extends Error{code;constructor(t,e){super(wr(t,e)),this.code=t}};function Nw(n){return`NG0${Math.abs(n)}`}function wr(n,t){return`${Nw(n)}${t?": "+t:""}`}var Cr=globalThis;function ke(n){for(let t in n)if(n[t]===ke)return t;throw Error("")}function nb(n,t){for(let e in t)t.hasOwnProperty(e)&&!n.hasOwnProperty(e)&&(n[e]=t[e])}function xc(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(xc).join(", ")}]`;if(n==null)return""+n;let t=n.overriddenName||n.name;if(t)return`${t}`;let e=n.toString();if(e==null)return""+e;let r=e.indexOf(`
`);return r>=0?e.slice(0,r):e}function Dc(n,t){return n?t?`${n} ${t}`:n:t||""}var Pw=ke({__forward_ref__:ke});function Ut(n){return n.__forward_ref__=Ut,n}function ft(n){return Fu(n)?n():n}function Fu(n){return typeof n=="function"&&n.hasOwnProperty(Pw)&&n.__forward_ref__===Ut}function C(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Be(n){return{providers:n.providers||[],imports:n.imports||[]}}function wc(n){return Fw(n,Cc)}function Fw(n,t){return n.hasOwnProperty(t)&&n[t]||null}function Lw(n){let t=n?.[Cc]??null;return t||null}function Eu(n){return n&&n.hasOwnProperty(pc)?n[pc]:null}var Cc=ke({\u0275prov:ke}),pc=ke({\u0275inj:ke}),x=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(t,e){this._desc=t,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=C({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Lu(n){return n&&!!n.\u0275providers}var Bu=ke({\u0275cmp:ke}),Vu=ke({\u0275dir:ke}),ju=ke({\u0275pipe:ke});var os=ke({\u0275fac:ke}),ai=ke({__NG_ELEMENT_ID__:ke}),Xg=ke({__NG_ENV_ID__:ke});function Er(n){return zu(n,"@Component"),n[Bu]||null}function Hu(n){return zu(n,"@Directive"),n[Vu]||null}function rb(n){return zu(n,"@Pipe"),n[ju]||null}function zu(n,t){if(n==null)throw new M(-919,!1)}function ds(n){return typeof n=="string"?n:n==null?"":String(n)}var ib=ke({ngErrorCode:ke}),Bw=ke({ngErrorMessage:ke}),Vw=ke({ngTokenPath:ke});function Uu(n,t){return ob("",-200,t)}function Ec(n,t){throw new M(-201,!1)}function ob(n,t,e){let r=new M(t,n);return r[ib]=t,r[Bw]=n,e&&(r[Vw]=e),r}function jw(n){return n[ib]}var ku;function sb(){return ku}function At(n){let t=ku;return ku=n,t}function $u(n,t,e){let r=wc(n);if(r&&r.providedIn=="root")return r.value===void 0?r.value=r.factory():r.value;if(e&8)return null;if(t!==void 0)return t;Ec(n,"")}var Hw={},ni=Hw,zw="__NG_DI_FLAG__",Iu=class{injector;constructor(t){this.injector=t}retrieve(t,e){let r=ri(e)||0;try{return this.injector.get(t,r&8?null:ni,r)}catch(i){if(oo(i))return i;throw i}}};function Uw(n,t=0){let e=mc();if(e===void 0)throw new M(-203,!1);if(e===null)return $u(n,void 0,t);{let r=$w(t),i=e.retrieve(n,r);if(oo(i)){if(r.optional)return null;throw i}return i}}function K(n,t=0){return(sb()||Uw)(ft(n),t)}function f(n,t){return K(n,ri(t))}function ri(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function $w(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function Su(n){let t=[];for(let e=0;e<n.length;e++){let r=ft(n[e]);if(Array.isArray(r)){if(r.length===0)throw new M(900,!1);let i,o=0;for(let s=0;s<r.length;s++){let a=r[s],c=Gw(a);typeof c=="number"?c===-1?i=a.token:o|=c:i=a}t.push(K(i,o))}else t.push(K(r))}return t}function Gw(n){return n[zw]}function ii(n,t){let e=n.hasOwnProperty(os);return e?n[os]:null}function ab(n,t,e){if(n.length!==t.length)return!1;for(let r=0;r<n.length;r++){let i=n[r],o=t[r];if(e&&(i=e(i),o=e(o)),o!==i)return!1}return!0}function cb(n){return n.flat(Number.POSITIVE_INFINITY)}function kc(n,t){n.forEach(e=>Array.isArray(e)?kc(e,t):t(e))}function Gu(n,t,e){t>=n.length?n.push(e):n.splice(t,0,e)}function us(n,t){return t>=n.length-1?n.pop():n.splice(t,1)[0]}function lb(n,t){let e=[];for(let r=0;r<n;r++)e.push(t);return e}function db(n,t,e,r){let i=n.length;if(i==t)n.push(e,r);else if(i===1)n.push(r,n[0]),n[0]=e;else{for(i--,n.push(n[i-1],n[i]);i>t;){let o=i-2;n[i]=n[o],i--}n[t]=e,n[t+1]=r}}function Ic(n,t,e){let r=ao(n,t);return r>=0?n[r|1]=e:(r=~r,db(n,r,t,e)),r}function Sc(n,t){let e=ao(n,t);if(e>=0)return n[e|1]}function ao(n,t){return Ww(n,t,1)}function Ww(n,t,e){let r=0,i=n.length>>e;for(;i!==r;){let o=r+(i-r>>1),s=n[o<<e];if(t===s)return o<<e;s>t?i=o:r=o+1}return~(i<<e)}var kr={},gt=[],ci=new x(""),Wu=new x("",-1),qu=new x(""),ss=class{get(t,e=ni){if(e===ni){let i=ob("",-201);throw i.name="\u0275NotFound",i}return e}};function co(n){return{\u0275providers:n}}function ub(n){return co([{provide:ci,multi:!0,useValue:n}])}function fb(...n){return{\u0275providers:Yu(!0,n),\u0275fromNgModule:!0}}function Yu(n,...t){let e=[],r=new Set,i,o=s=>{e.push(s)};return kc(t,s=>{let a=s;gc(a,o,[],r)&&(i||=[],i.push(a))}),i!==void 0&&mb(i,o),e}function mb(n,t){for(let e=0;e<n.length;e++){let{ngModule:r,providers:i}=n[e];Zu(i,o=>{t(o,r)})}}function gc(n,t,e,r){if(n=ft(n),!n)return!1;let i=null,o=Eu(n),s=!o&&Er(n);if(!o&&!s){let c=n.ngModule;if(o=Eu(c),o)i=c;else return!1}else{if(s&&!s.standalone)return!1;i=n}let a=r.has(i);if(s){if(a)return!1;if(r.add(i),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)gc(l,t,e,r)}}else if(o){if(o.imports!=null&&!a){r.add(i);let l;kc(o.imports,d=>{gc(d,t,e,r)&&(l||=[],l.push(d))}),l!==void 0&&mb(l,t)}if(!a){let l=ii(i)||(()=>new i);t({provide:i,useFactory:l,deps:gt},i),t({provide:qu,useValue:i,multi:!0},i),t({provide:ci,useValue:()=>K(i),multi:!0},i)}let c=o.providers;if(c!=null&&!a){let l=n;Zu(c,d=>{t(d,l)})}}else return!1;return i!==n&&n.providers!==void 0}function Zu(n,t){for(let e of n)Lu(e)&&(e=e.\u0275providers),Array.isArray(e)?Zu(e,t):t(e)}var qw=ke({provide:String,useValue:ke});function hb(n){return n!==null&&typeof n=="object"&&qw in n}function Yw(n){return!!(n&&n.useExisting)}function Zw(n){return!!(n&&n.useFactory)}function oi(n){return typeof n=="function"}function pb(n){return!!n.useClass}var fs=new x(""),hc={},Jg={},Cu;function lo(){return Cu===void 0&&(Cu=new ss),Cu}var qe=class{},si=class extends qe{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(t,e,r,i){super(),this.parent=e,this.source=r,this.scopes=i,Mu(t,s=>this.processProvider(s)),this.records.set(Wu,so(void 0,this)),i.has("environment")&&this.records.set(qe,so(void 0,this));let o=this.records.get(fs);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(qu,gt,{self:!0}))}retrieve(t,e){let r=ri(e)||0;try{return this.get(t,ni,r)}catch(i){if(oo(i))return i;throw i}}destroy(){is(this),this._destroyed=!0;let t=N(null);try{for(let r of this._ngOnDestroyHooks)r.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let r of e)r()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),N(t)}}onDestroy(t){return is(this),this._onDestroyHooks.push(t),()=>this.removeOnDestroy(t)}runInContext(t){is(this);let e=Rn(this),r=At(void 0),i;try{return t()}finally{Rn(e),At(r)}}get(t,e=ni,r){if(is(this),t.hasOwnProperty(Xg))return t[Xg](this);let i=ri(r),o,s=Rn(this),a=At(void 0);try{if(!(i&4)){let l=this.records.get(t);if(l===void 0){let d=eC(t)&&wc(t);d&&this.injectableDefInScope(d)?l=so(Tu(t),hc):l=null,this.records.set(t,l)}if(l!=null)return this.hydrate(t,l,i)}let c=i&2?lo():this.parent;return e=i&8&&e===ni?null:e,c.get(t,e)}catch(c){let l=jw(c);throw l===-200||l===-201?new M(l,null):c}finally{At(a),Rn(s)}}resolveInjectorInitializers(){let t=N(null),e=Rn(this),r=At(void 0),i;try{let o=this.get(ci,gt,{self:!0});for(let s of o)s()}finally{Rn(e),At(r),N(t)}}toString(){return"R3Injector[...]"}processProvider(t){t=ft(t);let e=oi(t)?t:ft(t&&t.provide),r=Qw(t);if(!oi(t)&&t.multi===!0){let i=this.records.get(e);i||(i=so(void 0,hc,!0),i.factory=()=>Su(i.multi),this.records.set(e,i)),e=t,i.multi.push(t)}this.records.set(e,r)}hydrate(t,e,r){let i=N(null);try{if(e.value===Jg)throw Uu("");return e.value===hc&&(e.value=Jg,e.value=e.factory(void 0,r)),typeof e.value=="object"&&e.value&&Jw(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{N(i)}}injectableDefInScope(t){if(!t.providedIn)return!1;let e=ft(t.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(t){let e=this._onDestroyHooks.indexOf(t);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Tu(n){let t=wc(n),e=t!==null?t.factory:ii(n);if(e!==null)return e;if(n instanceof x)throw new M(-204,!1);if(n instanceof Function)return Kw(n);throw new M(-204,!1)}function Kw(n){if(n.length>0)throw new M(-204,!1);let e=Lw(n);return e!==null?()=>e.factory(n):()=>new n}function Qw(n){if(hb(n))return so(void 0,n.useValue);{let t=Ku(n);return so(t,hc)}}function Ku(n,t,e){let r;if(oi(n)){let i=ft(n);return ii(i)||Tu(i)}else if(hb(n))r=()=>ft(n.useValue);else if(Zw(n))r=()=>n.useFactory(...Su(n.deps||[]));else if(Yw(n))r=(i,o)=>K(ft(n.useExisting),o!==void 0&&o&8?8:void 0);else{let i=ft(n&&(n.useClass||n.provide));if(Xw(n))r=()=>new i(...Su(n.deps));else return ii(i)||Tu(i)}return r}function is(n){if(n.destroyed)throw new M(-205,!1)}function so(n,t,e=!1){return{factory:n,value:t,multi:e?[]:void 0}}function Xw(n){return!!n.deps}function Jw(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function eC(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Mu(n,t){for(let e of n)Array.isArray(e)?Mu(e,t):e&&Lu(e)?Mu(e.\u0275providers,t):t(e)}function uo(n,t){let e;n instanceof si?(is(n),e=n):e=new Iu(n);let r,i=Rn(e),o=At(void 0);try{return t()}finally{Rn(i),At(o)}}function gb(){return sb()!==void 0||mc()!=null}var ln=0,L=1,$=2,rt=3,$t=4,Et=5,li=6,fo=7,Ye=8,Qn=9,dn=10,Se=11,mo=12,Qu=13,di=14,kt=15,Ir=16,ui=17,Nn=18,Xn=19,Xu=20,Kn=21,Tc=22,xr=23,Rt=24,fi=25,Sr=26,Ve=27,bb=1,Ju=6,Tr=7,ms=8,mi=9,ze=10;function Jn(n){return Array.isArray(n)&&typeof n[bb]=="object"}function un(n){return Array.isArray(n)&&n[bb]===!0}function ef(n){return(n.flags&4)!==0}function er(n){return n.componentOffset>-1}function ho(n){return(n.flags&1)===1}function Pn(n){return!!n.template}function po(n){return(n[$]&512)!==0}function hi(n){return(n[$]&256)===256}var tf="svg",yb="math";function Gt(n){for(;Array.isArray(n);)n=n[ln];return n}function nf(n,t){return Gt(t[n])}function fn(n,t){return Gt(t[n.index])}function Mc(n,t){return n.data[t]}function rf(n,t){return n[t]}function Ac(n,t,e,r){e>=n.data.length&&(n.data[e]=null,n.blueprint[e]=null),t[e]=r}function Wt(n,t){let e=t[n];return Jn(e)?e:e[ln]}function _b(n){return(n[$]&4)===4}function Rc(n){return(n[$]&128)===128}function vb(n){return un(n[rt])}function qt(n,t){return t==null?null:n[t]}function of(n){n[ui]=0}function sf(n){n[$]&1024||(n[$]|=1024,Rc(n)&&pi(n))}function xb(n,t){for(;n>0;)t=t[di],n--;return t}function hs(n){return!!(n[$]&9216||n[Rt]?.dirty)}function Oc(n){n[dn].changeDetectionScheduler?.notify(8),n[$]&64&&(n[$]|=1024),hs(n)&&pi(n)}function pi(n){n[dn].changeDetectionScheduler?.notify(0);let t=Dr(n);for(;t!==null&&!(t[$]&8192||(t[$]|=8192,!Rc(t)));)t=Dr(t)}function af(n,t){if(hi(n))throw new M(911,!1);n[Kn]===null&&(n[Kn]=[]),n[Kn].push(t)}function Db(n,t){if(n[Kn]===null)return;let e=n[Kn].indexOf(t);e!==-1&&n[Kn].splice(e,1)}function Dr(n){let t=n[rt];return un(t)?t[rt]:t}function cf(n){return n[fo]??=[]}function lf(n){return n.cleanup??=[]}function wb(n,t,e,r){let i=cf(t);i.push(e),n.firstCreatePass&&lf(n).push(r,i.length-1)}var ie={lFrame:Ob(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Au=!1;function Cb(){return ie.lFrame.elementDepthCount}function Eb(){ie.lFrame.elementDepthCount++}function df(){ie.lFrame.elementDepthCount--}function Nc(){return ie.bindingsEnabled}function uf(){return ie.skipHydrationRootTNode!==null}function ff(n){return ie.skipHydrationRootTNode===n}function mf(){ie.skipHydrationRootTNode=null}function H(){return ie.lFrame.lView}function Oe(){return ie.lFrame.tView}function Yt(n){return ie.lFrame.contextLView=n,n[Ye]}function Zt(n){return ie.lFrame.contextLView=null,n}function at(){let n=hf();for(;n!==null&&n.type===64;)n=n.parent;return n}function hf(){return ie.lFrame.currentTNode}function kb(){let n=ie.lFrame,t=n.currentTNode;return n.isParent?t:t.parent}function gi(n,t){let e=ie.lFrame;e.currentTNode=n,e.isParent=t}function pf(){return ie.lFrame.isParent}function gf(){ie.lFrame.isParent=!1}function bf(){return ie.lFrame.contextLView}function yf(){return Au}function as(n){let t=Au;return Au=n,t}function _f(){let n=ie.lFrame,t=n.bindingRootIndex;return t===-1&&(t=n.bindingRootIndex=n.tView.bindingStartIndex),t}function Ib(n){return ie.lFrame.bindingIndex=n}function Mr(){return ie.lFrame.bindingIndex++}function vf(n){let t=ie.lFrame,e=t.bindingIndex;return t.bindingIndex=t.bindingIndex+n,e}function Sb(){return ie.lFrame.inI18n}function Tb(n,t){let e=ie.lFrame;e.bindingIndex=e.bindingRootIndex=n,Pc(t)}function Mb(){return ie.lFrame.currentDirectiveIndex}function Pc(n){ie.lFrame.currentDirectiveIndex=n}function Ab(n){let t=ie.lFrame.currentDirectiveIndex;return t===-1?null:n[t]}function Fc(){return ie.lFrame.currentQueryIndex}function ps(n){ie.lFrame.currentQueryIndex=n}function tC(n){let t=n[L];return t.type===2?t.declTNode:t.type===1?n[Et]:null}function xf(n,t,e){if(e&4){let i=t,o=n;for(;i=i.parent,i===null&&!(e&1);)if(i=tC(o),i===null||(o=o[di],i.type&10))break;if(i===null)return!1;t=i,n=o}let r=ie.lFrame=Rb();return r.currentTNode=t,r.lView=n,!0}function Lc(n){let t=Rb(),e=n[L];ie.lFrame=t,t.currentTNode=e.firstChild,t.lView=n,t.tView=e,t.contextLView=n,t.bindingIndex=e.bindingStartIndex,t.inI18n=!1}function Rb(){let n=ie.lFrame,t=n===null?null:n.child;return t===null?Ob(n):t}function Ob(n){let t={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=t),t}function Nb(){let n=ie.lFrame;return ie.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Df=Nb;function Bc(){let n=Nb();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function Pb(n){return(ie.lFrame.contextLView=xb(n,ie.lFrame.contextLView))[Ye]}function Fn(){return ie.lFrame.selectedIndex}function Ar(n){ie.lFrame.selectedIndex=n}function gs(){let n=ie.lFrame;return Mc(n.tView,n.selectedIndex)}function mn(){ie.lFrame.currentNamespace=tf}function go(){nC()}function nC(){ie.lFrame.currentNamespace=null}function Fb(){return ie.lFrame.currentNamespace}var Lb=!0;function Vc(){return Lb}function bs(n){Lb=n}function Ru(n,t=null,e=null,r){let i=Bb(n,t,e,r);return i.resolveInjectorInitializers(),i}function Bb(n,t=null,e=null,r,i=new Set){let o=[e||gt,fb(n)],s;return new si(o,t||lo(),s||null,i)}var V=class n{static THROW_IF_NOT_FOUND=ni;static NULL=new ss;static create(t,e){if(Array.isArray(t))return Ru({name:""},e,t,"");{let r=t.name??"";return Ru({name:r},t.parent,t.providers,r)}}static \u0275prov=C({token:n,providedIn:"any",factory:()=>K(Wu)});static __NG_ELEMENT_ID__=-1},B=new x(""),Ot=(()=>{class n{static __NG_ELEMENT_ID__=rC;static __NG_ENV_ID__=e=>e}return n})(),bc=class extends Ot{_lView;constructor(t){super(),this._lView=t}get destroyed(){return hi(this._lView)}onDestroy(t){let e=this._lView;return af(e,t),()=>Db(e,t)}};function rC(){return new bc(H())}var Vb=!1,jb=new x(""),bi=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Qr(!1);debugTaskTracker=f(jb,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new le(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=C({token:n,providedIn:"root",factory:()=>new n})}return n})(),Ou=class extends E{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(t=!1){super(),this.__isAsync=t,gb()&&(this.destroyRef=f(Ot,{optional:!0})??void 0,this.pendingTasks=f(bi,{optional:!0})??void 0)}emit(t){let e=N(null);try{super.next(t)}finally{N(e)}}subscribe(t,e,r){let i=t,o=e||(()=>null),s=r;if(t&&typeof t=="object"){let c=t;i=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),i&&(i=this.wrapInTimeout(i)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:i,error:o,complete:s});return t instanceof ye&&t.add(a),a}wrapInTimeout(t){return e=>{let r=this.pendingTasks?.add();setTimeout(()=>{try{t(e)}finally{r!==void 0&&this.pendingTasks?.remove(r)}})}}},j=Ou;function yc(...n){}function wf(n){let t,e;function r(){n=yc;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),t!==void 0&&clearTimeout(t)}catch{}}return t=setTimeout(()=>{n(),r()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{n(),r()})),()=>r()}function Hb(n){return queueMicrotask(()=>n()),()=>{n=yc}}var Cf="isAngularZone",cs=Cf+"_ID",iC=0,F=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new j(!1);onMicrotaskEmpty=new j(!1);onStable=new j(!1);onError=new j(!1);constructor(t){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:r=!1,shouldCoalesceRunChangeDetection:i=!1,scheduleInRootZone:o=Vb}=t;if(typeof Zone>"u")throw new M(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!i&&r,s.shouldCoalesceRunChangeDetection=i,s.callbackScheduled=!1,s.scheduleInRootZone=o,aC(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Cf)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new M(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new M(909,!1)}run(t,e,r){return this._inner.run(t,e,r)}runTask(t,e,r,i){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+i,t,oC,yc,yc);try{return o.runTask(s,e,r)}finally{o.cancelTask(s)}}runGuarded(t,e,r){return this._inner.runGuarded(t,e,r)}runOutsideAngular(t){return this._outer.run(t)}},oC={};function Ef(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function sC(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function t(){wf(()=>{n.callbackScheduled=!1,Nu(n),n.isCheckStableRunning=!0,Ef(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{t()}):n._outer.run(()=>{t()}),Nu(n)}function aC(n){let t=()=>{sC(n)},e=iC++;n._inner=n._inner.fork({name:"angular",properties:{[Cf]:!0,[cs]:e,[cs+e]:!0},onInvokeTask:(r,i,o,s,a,c)=>{if(cC(c))return r.invokeTask(o,s,a,c);try{return eb(n),r.invokeTask(o,s,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&t(),tb(n)}},onInvoke:(r,i,o,s,a,c,l)=>{try{return eb(n),r.invoke(o,s,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!lC(c)&&t(),tb(n)}},onHasTask:(r,i,o,s)=>{r.hasTask(o,s),i===o&&(s.change=="microTask"?(n._hasPendingMicrotasks=s.microTask,Nu(n),Ef(n)):s.change=="macroTask"&&(n.hasPendingMacrotasks=s.macroTask))},onHandleError:(r,i,o,s)=>(r.handleError(o,s),n.runOutsideAngular(()=>n.onError.emit(s)),!1)})}function Nu(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function eb(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function tb(n){n._nesting--,Ef(n)}var ls=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new j;onMicrotaskEmpty=new j;onStable=new j;onError=new j;run(t,e,r){return t.apply(e,r)}runGuarded(t,e,r){return t.apply(e,r)}runOutsideAngular(t){return t()}runTask(t,e,r,i){return t.apply(e,r)}};function cC(n){return zb(n,"__ignore_ng_zone__")}function lC(n){return zb(n,"__scheduler_tick__")}function zb(n,t){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[t]===!0}var cn=class{_console=console;handleError(t){this._console.error("ERROR",t)}},tr=new x("",{factory:()=>{let n=f(F),t=f(qe),e;return r=>{n.runOutsideAngular(()=>{t.destroyed&&!e?setTimeout(()=>{throw r}):(e??=t.get(cn),e.handleError(r))})}}}),Ub={provide:ci,useValue:()=>{let n=f(cn,{optional:!0})},multi:!0},dC=new x("",{factory:()=>{let n=f(B).defaultView;if(!n)return;let t=f(tr),e=o=>{t(o.reason),o.preventDefault()},r=o=>{o.error?t(o.error):t(new Error(o.message,{cause:o})),o.preventDefault()},i=()=>{n.addEventListener("unhandledrejection",e),n.addEventListener("error",r)};typeof Zone<"u"?Zone.root.run(i):i(),f(Ot).onDestroy(()=>{n.removeEventListener("error",r),n.removeEventListener("unhandledrejection",e)})}});function kf(){return co([ub(()=>{f(dC)})])}function X(n,t){let[e,r,i]=ou(n,t?.equal),o=e,s=o[We];return o.set=r,o.update=i,o.asReadonly=ys.bind(o),o}function ys(){let n=this[We];if(n.readonlyFn===void 0){let t=()=>this();t[We]=n,n.readonlyFn=t}return n.readonlyFn}var bo=(()=>{class n{view;node;constructor(e,r){this.view=e,this.node=r}static __NG_ELEMENT_ID__=uC}return n})();function uC(){return new bo(H(),at())}var On=class{},_s=new x("",{factory:()=>!0});var If=new x(""),yo=(()=>{class n{internalPendingTasks=f(bi);scheduler=f(On);errorHandler=f(tr);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let r=this.add();e().catch(this.errorHandler).finally(r)}static \u0275prov=C({token:n,providedIn:"root",factory:()=>new n})}return n})(),jc=(()=>{class n{static \u0275prov=C({token:n,providedIn:"root",factory:()=>new Pu})}return n})(),Pu=class{dirtyEffectCount=0;queues=new Map;add(t){this.enqueue(t),this.schedule(t)}schedule(t){t.dirty&&this.dirtyEffectCount++}remove(t){let e=t.zone,r=this.queues.get(e);r.has(t)&&(r.delete(t),t.dirty&&this.dirtyEffectCount--)}enqueue(t){let e=t.zone;this.queues.has(e)||this.queues.set(e,new Set);let r=this.queues.get(e);r.has(t)||r.add(t)}flush(){for(;this.dirtyEffectCount>0;){let t=!1;for(let[e,r]of this.queues)e===null?t||=this.flushQueue(r):t||=e.run(()=>this.flushQueue(r));t||(this.dirtyEffectCount=0)}}flushQueue(t){let e=!1;for(let r of t)r.dirty&&(this.dirtyEffectCount--,e=!0,r.run());return e}},_c=class{[We];constructor(t){this[We]=t}destroy(){this[We].destroy()}};function bt(n,t){let e=t?.injector??f(V),r=t?.manualCleanup!==!0?e.get(Ot):null,i,o=e.get(bo,null,{optional:!0}),s=e.get(On);return o!==null?(i=hC(o.view,s,n),r instanceof bc&&r._lView===o.view&&(r=null)):i=pC(n,e.get(jc),s),i.injector=e,r!==null&&(i.onDestroyFns=[r.onDestroy(()=>i.destroy())]),new _c(i)}var $b=W(w({},su),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=as(!1);try{au(this)}finally{as(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=N(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],N(n)}}}),fC=W(w({},$b),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(_r(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),mC=W(w({},$b),{consumerMarkedDirty(){this.view[$]|=8192,pi(this.view),this.notifier.notify(13)},destroy(){if(_r(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[xr]?.delete(this)}});function hC(n,t,e){let r=Object.create(mC);return r.view=n,r.zone=typeof Zone<"u"?Zone.current:null,r.notifier=t,r.fn=Gb(r,e),n[xr]??=new Set,n[xr].add(r),r.consumerMarkedDirty(r),r}function pC(n,t,e){let r=Object.create(fC);return r.fn=Gb(r,n),r.scheduler=t,r.notifier=e,r.zone=typeof Zone<"u"?Zone.current:null,r.scheduler.add(r),r.notifier.notify(12),r}function Gb(n,t){return()=>{t(e=>(n.cleanupFns??=[]).push(e))}}function Ms(n){return{toString:n}.toString()}function CC(n){return typeof n=="function"}function Ey(n,t,e,r){t!==null?t.applyValueToInputSignal(t,r):n[e]=r}var Zc=class{previousValue;currentValue;firstChange;constructor(t,e,r){this.previousValue=t,this.currentValue=e,this.firstChange=r}isFirstChange(){return this.firstChange}},it=(()=>{let n=()=>ky;return n.ngInherit=!0,n})();function ky(n){return n.type.prototype.ngOnChanges&&(n.setInput=kC),EC}function EC(){let n=Sy(this),t=n?.current;if(t){let e=n.previous;if(e===kr)n.previous=t;else for(let r in t)e[r]=t[r];n.current=null,this.ngOnChanges(t)}}function kC(n,t,e,r,i){let o=this.declaredInputs[r],s=Sy(n)||IC(n,{previous:kr,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new Zc(l&&l.currentValue,e,c===kr),Ey(n,t,i,e)}var Iy="__ngSimpleChanges__";function Sy(n){return n[Iy]||null}function IC(n,t){return n[Iy]=t}var Wb=[];var Ee=function(n,t=null,e){for(let r=0;r<Wb.length;r++){let i=Wb[r];i(n,t,e)}},ve=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(ve||{});function SC(n,t,e){let{ngOnChanges:r,ngOnInit:i,ngDoCheck:o}=t.type.prototype;if(r){let s=ky(t);(e.preOrderHooks??=[]).push(n,s),(e.preOrderCheckHooks??=[]).push(n,s)}i&&(e.preOrderHooks??=[]).push(0-n,i),o&&((e.preOrderHooks??=[]).push(n,o),(e.preOrderCheckHooks??=[]).push(n,o))}function Ty(n,t){for(let e=t.directiveStart,r=t.directiveEnd;e<r;e++){let o=n.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(n.contentHooks??=[]).push(-e,s),a&&((n.contentHooks??=[]).push(e,a),(n.contentCheckHooks??=[]).push(e,a)),c&&(n.viewHooks??=[]).push(-e,c),l&&((n.viewHooks??=[]).push(e,l),(n.viewCheckHooks??=[]).push(e,l)),d!=null&&(n.destroyHooks??=[]).push(e,d)}}function Gc(n,t,e){My(n,t,3,e)}function Wc(n,t,e,r){(n[$]&3)===e&&My(n,t,e,r)}function Sf(n,t){let e=n[$];(e&3)===t&&(e&=16383,e+=1,n[$]=e)}function My(n,t,e,r){let i=r!==void 0?n[ui]&65535:0,o=r??-1,s=t.length-1,a=0;for(let c=i;c<s;c++)if(typeof t[c+1]=="number"){if(a=t[c],r!=null&&a>=r)break}else t[c]<0&&(n[ui]+=65536),(a<o||o==-1)&&(TC(n,e,t,c),n[ui]=(n[ui]&4294901760)+c+2),c++}function qb(n,t){Ee(ve.LifecycleHookStart,n,t);let e=N(null);try{t.call(n)}finally{N(e),Ee(ve.LifecycleHookEnd,n,t)}}function TC(n,t,e,r){let i=e[r]<0,o=e[r+1],s=i?-e[r]:e[r],a=n[s];i?n[$]>>14<n[ui]>>16&&(n[$]&3)===t&&(n[$]+=16384,qb(a,o)):qb(a,o)}var vo=-1,_i=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(t,e,r,i){this.factory=t,this.name=i,this.canSeeViewProviders=e,this.injectImpl=r}};function MC(n){return(n.flags&8)!==0}function AC(n){return(n.flags&16)!==0}function RC(n,t,e){let r=0;for(;r<e.length;){let i=e[r];if(typeof i=="number"){if(i!==0)break;r++;let o=e[r++],s=e[r++],a=e[r++];n.setAttribute(t,s,a,o)}else{let o=i,s=e[++r];OC(o)?n.setProperty(t,o,s):n.setAttribute(t,o,s),r++}}return r}function Ay(n){return n===3||n===4||n===6}function OC(n){return n.charCodeAt(0)===64}function xo(n,t){if(!(t===null||t.length===0))if(n===null||n.length===0)n=t.slice();else{let e=-1;for(let r=0;r<t.length;r++){let i=t[r];typeof i=="number"?e=i:e===0||(e===-1||e===2?Yb(n,e,i,null,t[++r]):Yb(n,e,i,null,null))}}return n}function Yb(n,t,e,r,i){let o=0,s=n.length;if(t===-1)s=-1;else for(;o<n.length;){let a=n[o++];if(typeof a=="number"){if(a===t){s=-1;break}else if(a>t){s=o-1;break}}}for(;o<n.length;){let a=n[o];if(typeof a=="number")break;if(a===e){i!==null&&(n[o+1]=i);return}o++,i!==null&&o++}s!==-1&&(n.splice(s,0,t),o=s+1),n.splice(o++,0,e),i!==null&&n.splice(o++,0,i)}function Ry(n){return n!==vo}function Kc(n){return n&32767}function NC(n){return n>>16}function Qc(n,t){let e=NC(n),r=t;for(;e>0;)r=r[di],e--;return r}var Vf=!0;function Zb(n){let t=Vf;return Vf=n,t}var PC=256,Oy=PC-1,Ny=5,FC=0,Ln={};function LC(n,t,e){let r;typeof e=="string"?r=e.charCodeAt(0)||0:e.hasOwnProperty(ai)&&(r=e[ai]),r==null&&(r=e[ai]=FC++);let i=r&Oy,o=1<<i;t.data[n+(i>>Ny)]|=o}function Xc(n,t){let e=Py(n,t);if(e!==-1)return e;let r=t[L];r.firstCreatePass&&(n.injectorIndex=t.length,Tf(r.data,n),Tf(t,null),Tf(r.blueprint,null));let i=wm(n,t),o=n.injectorIndex;if(Ry(i)){let s=Kc(i),a=Qc(i,t),c=a[L].data;for(let l=0;l<8;l++)t[o+l]=a[s+l]|c[s+l]}return t[o+8]=i,o}function Tf(n,t){n.push(0,0,0,0,0,0,0,0,t)}function Py(n,t){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||t[n.injectorIndex+8]===null?-1:n.injectorIndex}function wm(n,t){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let e=0,r=null,i=t;for(;i!==null;){if(r=jy(i),r===null)return vo;if(e++,i=i[di],r.injectorIndex!==-1)return r.injectorIndex|e<<16}return vo}function jf(n,t,e){LC(n,t,e)}function BC(n,t){if(t==="class")return n.classes;if(t==="style")return n.styles;let e=n.attrs;if(e){let r=e.length,i=0;for(;i<r;){let o=e[i];if(Ay(o))break;if(o===0)i=i+2;else if(typeof o=="number")for(i++;i<r&&typeof e[i]=="string";)i++;else{if(o===t)return e[i+1];i=i+2}}}return null}function Fy(n,t,e){if(e&8||n!==void 0)return n;Ec(t,"NodeInjector")}function Ly(n,t,e,r){if(e&8&&r===void 0&&(r=null),(e&3)===0){let i=n[Qn],o=At(void 0);try{return i?i.get(t,r,e&8):$u(t,r,e&8)}finally{At(o)}}return Fy(r,t,e)}function By(n,t,e,r=0,i){if(n!==null){if(t[$]&2048&&!(r&2)){let s=zC(n,t,e,r,Ln);if(s!==Ln)return s}let o=Vy(n,t,e,r,Ln);if(o!==Ln)return o}return Ly(t,e,r,i)}function Vy(n,t,e,r,i){let o=jC(e);if(typeof o=="function"){if(!xf(t,n,r))return r&1?Fy(i,e,r):Ly(t,e,r,i);try{let s;if(s=o(r),s==null&&!(r&8))Ec(e);else return s}finally{Df()}}else if(typeof o=="number"){let s=null,a=Py(n,t),c=vo,l=r&1?t[kt][Et]:null;for((a===-1||r&4)&&(c=a===-1?wm(n,t):t[a+8],c===vo||!Qb(r,!1)?a=-1:(s=t[L],a=Kc(c),t=Qc(c,t)));a!==-1;){let d=t[L];if(Kb(o,a,d.data)){let u=VC(a,t,e,s,r,l);if(u!==Ln)return u}c=t[a+8],c!==vo&&Qb(r,t[L].data[a+8]===l)&&Kb(o,a,t)?(s=d,a=Kc(c),t=Qc(c,t)):a=-1}}return i}function VC(n,t,e,r,i,o){let s=t[L],a=s.data[n+8],c=r==null?er(a)&&Vf:r!=s&&(a.type&3)!==0,l=i&1&&o===a,d=qc(a,s,e,c,l);return d!==null?ws(t,s,d,a,i):Ln}function qc(n,t,e,r,i){let o=n.providerIndexes,s=t.data,a=o&1048575,c=n.directiveStart,l=n.directiveEnd,d=o>>20,u=r?a:a+d,m=i?a+d:l;for(let h=u;h<m;h++){let p=s[h];if(h<c&&e===p||h>=c&&p.type===e)return h}if(i){let h=s[c];if(h&&Pn(h)&&h.type===e)return c}return null}function ws(n,t,e,r,i){let o=n[e],s=t.data;if(o instanceof _i){let a=o;if(a.resolving)throw Uu("");let c=Zb(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],d,u=a.injectImpl?At(a.injectImpl):null,m=xf(n,r,0);try{o=n[e]=a.factory(void 0,i,s,n,r),t.firstCreatePass&&e>=r.directiveStart&&SC(e,s[e],t)}finally{u!==null&&At(u),Zb(c),a.resolving=!1,Df()}}return o}function jC(n){if(typeof n=="string")return n.charCodeAt(0)||0;let t=n.hasOwnProperty(ai)?n[ai]:void 0;return typeof t=="number"?t>=0?t&Oy:HC:t}function Kb(n,t,e){let r=1<<n;return!!(e[t+(n>>Ny)]&r)}function Qb(n,t){return!(n&2)&&!(n&1&&t)}var yi=class{_tNode;_lView;constructor(t,e){this._tNode=t,this._lView=e}get(t,e,r){return By(this._tNode,this._lView,t,ri(r),e)}};function HC(){return new yi(at(),H())}function Kt(n){return Ms(()=>{let t=n.prototype.constructor,e=t[os]||Hf(t),r=Object.prototype,i=Object.getPrototypeOf(n.prototype).constructor;for(;i&&i!==r;){let o=i[os]||Hf(i);if(o&&o!==e)return o;i=Object.getPrototypeOf(i)}return o=>new o})}function Hf(n){return Fu(n)?()=>{let t=Hf(ft(n));return t&&t()}:ii(n)}function zC(n,t,e,r,i){let o=n,s=t;for(;o!==null&&s!==null&&s[$]&2048&&!po(s);){let a=Vy(o,s,e,r|2,Ln);if(a!==Ln)return a;let c=o.parent;if(!c){let l=s[Xu];if(l){let d=l.get(e,Ln,r&-5);if(d!==Ln)return d}c=jy(s),s=s[di]}o=c}return i}function jy(n){let t=n[L],e=t.type;return e===2?t.declTNode:e===1?n[Et]:null}function Cm(n){return BC(at(),n)}function UC(){return ko(at(),H())}function ko(n,t){return new z(fn(n,t))}var z=(()=>{class n{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=UC}return n})();function Hy(n){return n instanceof z?n.nativeElement:n}function $C(){return this._results[Symbol.iterator]()}var nr=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new E}constructor(t=!1){this._emitDistinctChangesOnly=t}get(t){return this._results[t]}map(t){return this._results.map(t)}filter(t){return this._results.filter(t)}find(t){return this._results.find(t)}reduce(t,e){return this._results.reduce(t,e)}forEach(t){this._results.forEach(t)}some(t){return this._results.some(t)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(t,e){this.dirty=!1;let r=cb(t);(this._changesDetected=!ab(this._results,r,e))&&(this._results=r,this.length=r.length,this.last=r[this.length-1],this.first=r[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(t){this._onDirty=t}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=$C};function zy(n){return(n.flags&128)===128}var Em=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(Em||{}),Uy=new Map,GC=0;function WC(){return GC++}function qC(n){Uy.set(n[Xn],n)}function zf(n){Uy.delete(n[Xn])}var Xb="__ngContext__";function Do(n,t){Jn(t)?(n[Xb]=t[Xn],qC(t)):n[Xb]=t}function $y(n){return Wy(n[mo])}function Gy(n){return Wy(n[$t])}function Wy(n){for(;n!==null&&!un(n);)n=n[$t];return n}var Uf;function km(n){Uf=n}function qy(){if(Uf!==void 0)return Uf;if(typeof document<"u")return document;throw new M(210,!1)}var Di=new x("",{factory:()=>YC}),YC="ng";var al=new x(""),wi=new x("",{providedIn:"platform",factory:()=>"unknown"}),As=new x(""),Ci=new x("",{factory:()=>f(B).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Im=(()=>{class n{static \u0275prov=C({token:n,providedIn:"root",factory:()=>{let e=new n;return e.store=ZC(f(B),f(Di)),e}});store={};onSerializeCallbacks={};get(e,r){return this.store[e]!==void 0?this.store[e]:r}set(e,r){this.store[e]=r}remove(e){delete this.store[e]}hasKey(e){return this.store.hasOwnProperty(e)}get isEmpty(){return Object.keys(this.store).length===0}onSerialize(e,r){this.onSerializeCallbacks[e]=r}toJson(){for(let e in this.onSerializeCallbacks)if(this.onSerializeCallbacks.hasOwnProperty(e))try{this.store[e]=this.onSerializeCallbacks[e]()}catch(r){console.warn("Exception in onSerialize callback: ",r)}return JSON.stringify(this.store).replace(/</g,"\\u003C").replace(/\//g,"\\u002F")}}return n})();function ZC(n,t){let e=n.getElementById(t+"-state");if(e?.textContent)try{return JSON.parse(e.textContent)}catch(r){console.warn("Exception while restoring TransferState for app "+t,r)}return{}}var Yy="r";var Zy="di";var Ky=!1,Qy=new x("",{factory:()=>Ky});var Jb=new WeakMap;function KC(n,t){if(n==null||typeof n!="object")return;let e=Jb.get(n);e||(e=new WeakSet,Jb.set(n,e)),e.add(t)}var QC=(n,t,e,r)=>{};function XC(n,t,e,r){QC(n,t,e,r)}function cl(n){return(n.flags&32)===32}var JC=()=>null;function Xy(n,t,e=!1){return JC(n,t,e)}function Jy(n,t){let e=n.contentQueries;if(e!==null){let r=N(null);try{for(let i=0;i<e.length;i+=2){let o=e[i],s=e[i+1];if(s!==-1){let a=n.data[s];ps(o),a.contentQueries(2,t[s],s)}}}finally{N(r)}}}function $f(n,t,e){ps(0);let r=N(null);try{t(n,e)}finally{N(r)}}function Sm(n,t,e){if(ef(t)){let r=N(null);try{let i=t.directiveStart,o=t.directiveEnd;for(let s=i;s<o;s++){let a=n.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{N(r)}}}var gn=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(gn||{});var Hc;function eE(){if(Hc===void 0&&(Hc=null,Cr.trustedTypes))try{Hc=Cr.trustedTypes.createPolicy("angular",{createHTML:n=>n,createScript:n=>n,createScriptURL:n=>n})}catch{}return Hc}function ll(n){return eE()?.createHTML(n)||n}var zc;function tE(){if(zc===void 0&&(zc=null,Cr.trustedTypes))try{zc=Cr.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:n=>n,createScript:n=>n,createScriptURL:n=>n})}catch{}return zc}function ey(n){return tE()?.createHTML(n)||n}var rr=class{changingThisBreaksApplicationSecurity;constructor(t){this.changingThisBreaksApplicationSecurity=t}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${vc})`}},Gf=class extends rr{getTypeName(){return"HTML"}},Wf=class extends rr{getTypeName(){return"Style"}},qf=class extends rr{getTypeName(){return"Script"}},Yf=class extends rr{getTypeName(){return"URL"}},Zf=class extends rr{getTypeName(){return"ResourceURL"}};function bn(n){return n instanceof rr?n.changingThisBreaksApplicationSecurity:n}function ir(n,t){let e=e_(n);if(e!=null&&e!==t){if(e==="ResourceURL"&&t==="URL")return!0;throw new Error(`Required a safe ${t}, got a ${e} (see ${vc})`)}return e===t}function e_(n){return n instanceof rr&&n.getTypeName()||null}function Tm(n){return new Gf(n)}function Mm(n){return new Wf(n)}function Am(n){return new qf(n)}function Rm(n){return new Yf(n)}function Om(n){return new Zf(n)}function nE(n){let t=new Qf(n);return rE()?new Kf(t):t}var Kf=class{inertDocumentHelper;constructor(t){this.inertDocumentHelper=t}getInertBodyElement(t){t="<body><remove></remove>"+t;try{let e=new window.DOMParser().parseFromString(ll(t),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(t):(e.firstChild?.remove(),e)}catch{return null}}},Qf=class{defaultDoc;inertDocument;constructor(t){this.defaultDoc=t,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(t){let e=this.inertDocument.createElement("template");return e.innerHTML=ll(t),e}};function rE(){try{return!!new window.DOMParser().parseFromString(ll(""),"text/html")}catch{return!1}}var iE=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Rs(n){return n=String(n),n.match(iE)?n:"unsafe:"+n}function or(n){let t={};for(let e of n.split(","))t[e]=!0;return t}function Os(...n){let t={};for(let e of n)for(let r in e)e.hasOwnProperty(r)&&(t[r]=!0);return t}var t_=or("area,br,col,hr,img,wbr"),n_=or("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),r_=or("rp,rt"),oE=Os(r_,n_),sE=Os(n_,or("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),aE=Os(r_,or("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),ty=Os(t_,sE,aE,oE),i_=or("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),cE=or("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),lE=or("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),dE=Os(i_,cE,lE),uE=or("script,style,template");var Xf=class{sanitizedSomething=!1;buf=[];sanitizeChildren(t){let e=t.firstChild,r=!0,i=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?r=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,r&&e.firstChild){i.push(e),e=hE(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=mE(e);if(o){e=o;break}e=i.pop()}}return this.buf.join("")}startElement(t){let e=ny(t).toLowerCase();if(!ty.hasOwnProperty(e))return this.sanitizedSomething=!0,!uE.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let r=t.attributes;for(let i=0;i<r.length;i++){let o=r.item(i),s=o.name,a=s.toLowerCase();if(!dE.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;i_[a]&&(c=Rs(c)),this.buf.push(" ",s,'="',ry(c),'"')}return this.buf.push(">"),!0}endElement(t){let e=ny(t).toLowerCase();ty.hasOwnProperty(e)&&!t_.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(t){this.buf.push(ry(t))}};function fE(n,t){return(n.compareDocumentPosition(t)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function mE(n){let t=n.nextSibling;if(t&&n!==t.previousSibling)throw o_(t);return t}function hE(n){let t=n.firstChild;if(t&&fE(n,t))throw o_(t);return t}function ny(n){let t=n.nodeName;return typeof t=="string"?t:"FORM"}function o_(n){return new Error(`Failed to sanitize html because the element is clobbered: ${n.outerHTML}`)}var pE=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,gE=/([^\#-~ |!])/g;function ry(n){return n.replace(/&/g,"&amp;").replace(pE,function(t){let e=t.charCodeAt(0),r=t.charCodeAt(1);return"&#"+((e-55296)*1024+(r-56320)+65536)+";"}).replace(gE,function(t){return"&#"+t.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Uc;function dl(n,t){let e=null;try{Uc=Uc||nE(n);let r=t?String(t):"";e=Uc.getInertBodyElement(r);let i=5,o=r;do{if(i===0)throw new Error("Failed to sanitize html because the input is unstable");i--,r=o,o=e.innerHTML,e=Uc.getInertBodyElement(r)}while(r!==o);let a=new Xf().sanitizeChildren(iy(e)||e);return ll(a)}finally{if(e){let r=iy(e)||e;for(;r.firstChild;)r.firstChild.remove()}}}function iy(n){return"content"in n&&bE(n)?n.content:null}function bE(n){return n.nodeType===Node.ELEMENT_NODE&&n.nodeName==="TEMPLATE"}var yE=/^>|^->|<!--|-->|--!>|<!-$/g,_E=/(<|>)/g,vE="\u200B$1\u200B";function xE(n){return n.replace(yE,t=>t.replace(_E,vE))}function DE(n,t){return n.createText(t)}function wE(n,t,e){n.setValue(t,e)}function CE(n,t){return n.createComment(xE(t))}function s_(n,t,e){return n.createElement(t,e)}function Jc(n,t,e,r,i){n.insertBefore(t,e,r,i)}function a_(n,t,e){n.appendChild(t,e)}function oy(n,t,e,r,i){r!==null?Jc(n,t,e,r,i):a_(n,t,e)}function c_(n,t,e,r){n.removeChild(null,t,e,r)}function EE(n,t,e){n.setAttribute(t,"style",e)}function kE(n,t,e){e===""?n.removeAttribute(t,"class"):n.setAttribute(t,"class",e)}function l_(n,t,e){let{mergedAttrs:r,classes:i,styles:o}=e;r!==null&&RC(n,t,r),i!==null&&kE(n,t,i),o!==null&&EE(n,t,o)}var Nt=(function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n})(Nt||{});function Ei(n){let t=d_();return t?ey(t.sanitize(Nt.HTML,n)||""):ir(n,"HTML")?ey(bn(n)):dl(qy(),ds(n))}function ul(n){let t=d_();return t?t.sanitize(Nt.URL,n)||"":ir(n,"URL")?bn(n):Rs(ds(n))}function d_(){let n=H();return n&&n[dn].sanitizer}function Nm(n){return n.ownerDocument}function IE(n,t,e){let r=n.length;for(;;){let i=n.indexOf(t,e);if(i===-1)return i;if(i===0||n.charCodeAt(i-1)<=32){let o=t.length;if(i+o===r||n.charCodeAt(i+o)<=32)return i}e=i+1}}var u_="ng-template";function SE(n,t,e,r){let i=0;if(r){for(;i<t.length&&typeof t[i]=="string";i+=2)if(t[i]==="class"&&IE(t[i+1].toLowerCase(),e,0)!==-1)return!0}else if(Pm(n))return!1;if(i=t.indexOf(1,i),i>-1){let o;for(;++i<t.length&&typeof(o=t[i])=="string";)if(o.toLowerCase()===e)return!0}return!1}function Pm(n){return n.type===4&&n.value!==u_}function TE(n,t,e){let r=n.type===4&&!e?u_:n.value;return t===r}function ME(n,t,e){let r=4,i=n.attrs,o=i!==null?OE(i):0,s=!1;for(let a=0;a<t.length;a++){let c=t[a];if(typeof c=="number"){if(!s&&!hn(r)&&!hn(c))return!1;if(s&&hn(c))continue;s=!1,r=c|r&1;continue}if(!s)if(r&4){if(r=2|r&1,c!==""&&!TE(n,c,e)||c===""&&t.length===1){if(hn(r))return!1;s=!0}}else if(r&8){if(i===null||!SE(n,i,c,e)){if(hn(r))return!1;s=!0}}else{let l=t[++a],d=AE(c,i,Pm(n),e);if(d===-1){if(hn(r))return!1;s=!0;continue}if(l!==""){let u;if(d>o?u="":u=i[d+1].toLowerCase(),r&2&&l!==u){if(hn(r))return!1;s=!0}}}}return hn(r)||s}function hn(n){return(n&1)===0}function AE(n,t,e,r){if(t===null)return-1;let i=0;if(r||!e){let o=!1;for(;i<t.length;){let s=t[i];if(s===n)return i;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=t[++i];for(;typeof a=="string";)a=t[++i];continue}else{if(s===4)break;if(s===0){i+=4;continue}}i+=o?1:2}return-1}else return NE(t,n)}function f_(n,t,e=!1){for(let r=0;r<t.length;r++)if(ME(n,t[r],e))return!0;return!1}function RE(n){let t=n.attrs;if(t!=null){let e=t.indexOf(5);if((e&1)===0)return t[e+1]}return null}function OE(n){for(let t=0;t<n.length;t++){let e=n[t];if(Ay(e))return t}return n.length}function NE(n,t){let e=n.indexOf(4);if(e>-1)for(e++;e<n.length;){let r=n[e];if(typeof r=="number")return-1;if(r===t)return e;e++}return-1}function PE(n,t){e:for(let e=0;e<t.length;e++){let r=t[e];if(n.length===r.length){for(let i=0;i<n.length;i++)if(n[i]!==r[i])continue e;return!0}}return!1}function sy(n,t){return n?":not("+t.trim()+")":t}function FE(n){let t=n[0],e=1,r=2,i="",o=!1;for(;e<n.length;){let s=n[e];if(typeof s=="string")if(r&2){let a=n[++e];i+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else r&8?i+="."+s:r&4&&(i+=" "+s);else i!==""&&!hn(s)&&(t+=sy(o,i),i=""),r=s,o=o||!hn(r);e++}return i!==""&&(t+=sy(o,i)),t}function LE(n){return n.map(FE).join(",")}function BE(n){let t=[],e=[],r=1,i=2;for(;r<n.length;){let o=n[r];if(typeof o=="string")i===2?o!==""&&t.push(o,n[++r]):i===8&&e.push(o);else{if(!hn(i))break;i=o}r++}return e.length&&t.push(1,...e),t}var Pt={};function Fm(n,t,e,r,i,o,s,a,c,l,d){let u=Ve+r,m=u+i,h=VE(u,m),p=typeof l=="function"?l():l;return h[L]={type:n,blueprint:h,template:e,queries:null,viewQuery:a,declTNode:t,data:h.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:m,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:p,incompleteFirstPass:!1,ssrId:d}}function VE(n,t){let e=[];for(let r=0;r<t;r++)e.push(r<n?null:Pt);return e}function jE(n){let t=n.tView;return t===null||t.incompleteFirstPass?n.tView=Fm(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):t}function Lm(n,t,e,r,i,o,s,a,c,l,d){let u=t.blueprint.slice();return u[ln]=i,u[$]=r|4|128|8|64|1024,(l!==null||n&&n[$]&2048)&&(u[$]|=2048),of(u),u[rt]=u[di]=n,u[Ye]=e,u[dn]=s||n&&n[dn],u[Se]=a||n&&n[Se],u[Qn]=c||n&&n[Qn]||null,u[Et]=o,u[Xn]=WC(),u[li]=d,u[Xu]=l,u[kt]=t.type==2?n[kt]:u,u}function HE(n,t,e){let r=fn(t,n),i=jE(e),o=n[dn].rendererFactory,s=Bm(n,Lm(n,i,null,m_(e),r,t,null,o.createRenderer(r,e),null,null,null));return n[t.index]=s}function m_(n){let t=16;return n.signals?t=4096:n.onPush&&(t=64),t}function h_(n,t,e,r){if(e===0)return-1;let i=t.length;for(let o=0;o<e;o++)t.push(r),n.blueprint.push(r),n.data.push(null);return i}function Bm(n,t){return n[mo]?n[Qu][$t]=t:n[mo]=t,n[Qu]=t,t}function S(n=1){p_(Oe(),H(),Fn()+n,!1)}function p_(n,t,e,r){if(!r)if((t[$]&3)===3){let o=n.preOrderCheckHooks;o!==null&&Gc(t,o,e)}else{let o=n.preOrderHooks;o!==null&&Wc(t,o,0,e)}Ar(e)}var fl=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(fl||{});function Jf(n,t,e,r){let i=N(null);try{let[o,s,a]=n.inputs[e],c=null;(s&fl.SignalBased)!==0&&(c=t[o][We]),c!==null&&c.transformFn!==void 0?r=c.transformFn(r):a!==null&&(r=a.call(t,r)),n.setInput!==null?n.setInput(t,c,r,e,o):Ey(t,c,o,r)}finally{N(i)}}var Bn=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(Bn||{}),zE;function Vm(n,t){return zE(n,t)}var cj=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var em=new WeakMap,vs=new WeakSet;function UE(n,t){let e=em.get(n);if(!e||e.length===0)return;let r=t.parentNode,i=t.previousSibling;for(let o=e.length-1;o>=0;o--){let s=e[o],a=s.parentNode;s===t?(e.splice(o,1),vs.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(i&&s===i||a&&r&&a!==r)&&(e.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function $E(n,t){let e=em.get(n);e?e.includes(t)||e.push(t):em.set(n,[t])}var vi=new Set,ml=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(ml||{}),yn=new x(""),ay=new Set;function sr(n){ay.has(n)||(ay.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var hl=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=C({token:n,providedIn:"root",factory:()=>new n})}return n})(),jm=[0,1,2,3],Hm=(()=>{class n{ngZone=f(F);scheduler=f(On);errorHandler=f(cn,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){f(yn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Ee(ve.AfterRenderHooksStart),this.executing=!0;for(let r of jm)for(let i of this.sequences)if(!(i.erroredOrDestroyed||!i.hooks[r]))try{i.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=i.hooks[r];return o(i.pipelinedValue)},i.snapshot))}catch(o){i.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let r of this.sequences)r.afterRun(),r.once&&(this.sequences.delete(r),r.destroy());for(let r of this.deferredRegistrations)this.sequences.add(r);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Ee(ve.AfterRenderHooksEnd)}register(e){let{view:r}=e;r!==void 0?((r[fi]??=[]).push(e),pi(r),r[$]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,r){return r?r.run(ml.AFTER_NEXT_RENDER,e):e()}static \u0275prov=C({token:n,providedIn:"root",factory:()=>new n})}return n})(),Cs=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(t,e,r,i,o,s=null){this.impl=t,this.hooks=e,this.view=r,this.once=i,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let t=this.view?.[fi];t&&(this.view[fi]=t.filter(e=>e!==this))}};function Ft(n,t){let e=t?.injector??f(V);return sr("NgAfterNextRender"),WE(n,e,t,!0)}function GE(n){return n instanceof Function?[void 0,void 0,n,void 0]:[n.earlyRead,n.write,n.mixedReadWrite,n.read]}function WE(n,t,e,r){let i=t.get(hl);i.impl??=t.get(Hm);let o=t.get(yn,null,{optional:!0}),s=e?.manualCleanup!==!0?t.get(Ot):null,a=t.get(bo,null,{optional:!0}),c=new Cs(i.impl,GE(n),a?.view,r,s,o?.snapshot(null));return i.impl.register(c),c}var g_=new x("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:f(qe)})});function b_(n,t,e){let r=n.get(g_);if(Array.isArray(t))for(let i of t)r.queue.add(i),e?.detachedLeaveAnimationFns?.push(i);else r.queue.add(t),e?.detachedLeaveAnimationFns?.push(t);r.scheduler&&r.scheduler(n)}function qE(n,t){let e=n.get(g_);if(t.detachedLeaveAnimationFns){for(let r of t.detachedLeaveAnimationFns)e.queue.delete(r);t.detachedLeaveAnimationFns=void 0}}function YE(n,t){for(let[e,r]of t)b_(n,r.animateFns)}function cy(n,t,e,r){let i=n?.[Sr]?.enter;t!==null&&i&&i.has(e.index)&&YE(r,i)}function _o(n,t,e,r,i,o,s,a){if(i!=null){let c,l=!1;un(i)?c=i:Jn(i)&&(l=!0,i=i[ln]);let d=Gt(i);n===0&&r!==null?(cy(a,r,o,e),s==null?a_(t,r,d):Jc(t,r,d,s||null,!0)):n===1&&r!==null?(cy(a,r,o,e),Jc(t,r,d,s||null,!0),UE(o,d)):n===2?(a?.[Sr]?.leave?.has(o.index)&&$E(o,d),vs.delete(d),ly(a,o,e,u=>{if(vs.has(d)){vs.delete(d);return}c_(t,d,l,u)})):n===3&&(vs.delete(d),ly(a,o,e,()=>{t.destroyNode(d)})),c!=null&&ok(t,n,e,c,o,r,s)}}function ZE(n,t){y_(n,t),t[ln]=null,t[Et]=null}function KE(n,t,e,r,i,o){r[ln]=i,r[Et]=t,gl(n,r,e,1,i,o)}function y_(n,t){t[dn].changeDetectionScheduler?.notify(9),gl(n,t,t[Se],2,null,null)}function QE(n){let t=n[mo];if(!t)return Mf(n[L],n);for(;t;){let e=null;if(Jn(t))e=t[mo];else{let r=t[ze];r&&(e=r)}if(!e){for(;t&&!t[$t]&&t!==n;)Jn(t)&&Mf(t[L],t),t=t[rt];t===null&&(t=n),Jn(t)&&Mf(t[L],t),e=t&&t[$t]}t=e}}function zm(n,t){let e=n[mi],r=e.indexOf(t);e.splice(r,1)}function pl(n,t){if(hi(t))return;let e=t[Se];e.destroyNode&&gl(n,t,e,3,null,null),QE(t)}function Mf(n,t){if(hi(t))return;let e=N(null);try{t[$]&=-129,t[$]|=256,t[Rt]&&_r(t[Rt]),ek(n,t),JE(n,t),t[L].type===1&&t[Se].destroy();let r=t[Ir];if(r!==null&&un(t[rt])){r!==t[rt]&&zm(r,t);let i=t[Nn];i!==null&&i.detachView(n)}zf(t)}finally{N(e)}}function ly(n,t,e,r){let i=n?.[Sr];if(i==null||i.leave==null||!i.leave.has(t.index))return r(!1);n&&vi.add(n[Xn]),b_(e,()=>{if(i.leave&&i.leave.has(t.index)){let s=i.leave.get(t.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:d}=l();a.push(d)}i.detachedLeaveAnimationFns=void 0}i.running=Promise.allSettled(a),XE(n,r)}else n&&vi.delete(n[Xn]),r(!1)},i)}function XE(n,t){let e=n[Sr]?.running;if(e){e.then(()=>{n[Sr].running=void 0,vi.delete(n[Xn]),t(!0)});return}t(!1)}function JE(n,t){let e=n.cleanup,r=t[fo];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?r[a]():r[-a].unsubscribe(),s+=2}else{let a=r[e[s+1]];e[s].call(a)}r!==null&&(t[fo]=null);let i=t[Kn];if(i!==null){t[Kn]=null;for(let s=0;s<i.length;s++){let a=i[s];a()}}let o=t[xr];if(o!==null){t[xr]=null;for(let s of o)s.destroy()}}function ek(n,t){let e;if(n!=null&&(e=n.destroyHooks)!=null)for(let r=0;r<e.length;r+=2){let i=t[e[r]];if(!(i instanceof _i)){let o=e[r+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=i[o[s]],c=o[s+1];Ee(ve.LifecycleHookStart,a,c);try{c.call(a)}finally{Ee(ve.LifecycleHookEnd,a,c)}}else{Ee(ve.LifecycleHookStart,i,o);try{o.call(i)}finally{Ee(ve.LifecycleHookEnd,i,o)}}}}}function __(n,t,e){return tk(n,t.parent,e)}function tk(n,t,e){let r=t;for(;r!==null&&r.type&168;)t=r,r=t.parent;if(r===null)return e[ln];if(er(r)){let{encapsulation:i}=n.data[r.directiveStart+r.componentOffset];if(i===gn.None||i===gn.Emulated)return null}return fn(r,e)}function v_(n,t,e){return rk(n,t,e)}function nk(n,t,e){return n.type&40?fn(n,e):null}var rk=nk,dy;function Um(n,t,e,r){let i=__(n,r,t),o=t[Se],s=r.parent||t[Et],a=v_(s,r,t);if(i!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)oy(o,i,e[c],a,!1);else oy(o,i,e,a,!1);dy!==void 0&&dy(o,r,t,e,i)}function xs(n,t){if(t!==null){let e=t.type;if(e&3)return fn(t,n);if(e&4)return tm(-1,n[t.index]);if(e&8){let r=t.child;if(r!==null)return xs(n,r);{let i=n[t.index];return un(i)?tm(-1,i):Gt(i)}}else{if(e&128)return xs(n,t.next);if(e&32)return Vm(t,n)()||Gt(n[t.index]);{let r=x_(n,t);if(r!==null){if(Array.isArray(r))return r[0];let i=Dr(n[kt]);return xs(i,r)}else return xs(n,t.next)}}}return null}function x_(n,t){if(t!==null){let r=n[kt][Et],i=t.projection;return r.projection[i]}return null}function tm(n,t){let e=ze+n+1;if(e<t.length){let r=t[e],i=r[L].firstChild;if(i!==null)return xs(r,i)}return t[Tr]}function $m(n,t,e,r,i,o,s){for(;e!=null;){let a=r[Qn];if(e.type===128){e=e.next;continue}let c=r[e.index],l=e.type;if(s&&t===0&&(c&&Do(Gt(c),r),e.flags|=2),!cl(e))if(l&8)$m(n,t,e.child,r,i,o,!1),_o(t,n,a,i,c,e,o,r);else if(l&32){let d=Vm(e,r),u;for(;u=d();)_o(t,n,a,i,u,e,o,r);_o(t,n,a,i,c,e,o,r)}else l&16?D_(n,t,r,e,i,o):_o(t,n,a,i,c,e,o,r);e=s?e.projectionNext:e.next}}function gl(n,t,e,r,i,o){$m(e,r,n.firstChild,t,i,o,!1)}function ik(n,t,e){let r=t[Se],i=__(n,e,t),o=e.parent||t[Et],s=v_(o,e,t);D_(r,0,t,e,i,s)}function D_(n,t,e,r,i,o){let s=e[kt],c=s[Et].projection[r.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];_o(t,n,e[Qn],i,d,r,o,e)}else{let l=c,d=s[rt];zy(r)&&(l.flags|=128),$m(n,t,l,d,i,o,!0)}}function ok(n,t,e,r,i,o,s){let a=r[Tr],c=Gt(r);a!==c&&_o(t,n,e,o,a,i,s);for(let l=ze;l<r.length;l++){let d=r[l];gl(d[L],d,n,t,o,a)}}function sk(n,t,e,r,i){if(t)i?n.addClass(e,r):n.removeClass(e,r);else{let o=r.indexOf("-")===-1?void 0:Bn.DashCase;i==null?n.removeStyle(e,r,o):(typeof i=="string"&&i.endsWith("!important")&&(i=i.slice(0,-10),o|=Bn.Important),n.setStyle(e,r,i,o))}}function w_(n,t,e,r,i){let o=Fn(),s=r&2;try{Ar(-1),s&&t.length>Ve&&p_(n,t,Ve,!1);let a=s?ve.TemplateUpdateStart:ve.TemplateCreateStart;Ee(a,i,e),e(r,i)}finally{Ar(o);let a=s?ve.TemplateUpdateEnd:ve.TemplateCreateEnd;Ee(a,i,e)}}function bl(n,t,e){fk(n,t,e),(e.flags&64)===64&&mk(n,t,e)}function Ns(n,t,e=fn){let r=t.localNames;if(r!==null){let i=t.index+1;for(let o=0;o<r.length;o+=2){let s=r[o+1],a=s===-1?e(t,n):n[s];n[i++]=a}}}function ak(n,t,e,r){let o=r.get(Qy,Ky)||e===gn.ShadowDom||e===gn.ExperimentalIsolatedShadowDom,s=n.selectRootElement(t,o);return ck(s),s}function ck(n){lk(n)}var lk=()=>null;function dk(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function C_(n,t,e,r,i,o){let s=t[L];if(Ym(n,s,t,e,r)){er(n)&&uk(t,n.index);return}n.type&3&&(e=dk(e)),E_(n,t,e,r,i,o)}function E_(n,t,e,r,i,o){if(n.type&3){let s=fn(n,t);r=o!=null?o(r,n.value||"",e):r,i.setProperty(s,e,r)}else n.type&12}function uk(n,t){let e=Wt(t,n);e[$]&16||(e[$]|=64)}function fk(n,t,e){let r=e.directiveStart,i=e.directiveEnd;er(e)&&HE(t,e,n.data[r+e.componentOffset]),n.firstCreatePass||Xc(e,t);let o=e.initialInputs;for(let s=r;s<i;s++){let a=n.data[s],c=ws(t,n,s,e);if(Do(c,t),o!==null&&bk(t,s-r,c,a,e,o),Pn(a)){let l=Wt(e.index,t);l[Ye]=ws(t,n,s,e)}}}function mk(n,t,e){let r=e.directiveStart,i=e.directiveEnd,o=e.index,s=Mb();try{Ar(o);for(let a=r;a<i;a++){let c=n.data[a],l=t[a];Pc(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&hk(c,l)}}finally{Ar(-1),Pc(s)}}function hk(n,t){n.hostBindings!==null&&n.hostBindings(1,t)}function Gm(n,t){let e=n.directiveRegistry,r=null;if(e)for(let i=0;i<e.length;i++){let o=e[i];f_(t,o.selectors,!1)&&(r??=[],Pn(o)?r.unshift(o):r.push(o))}return r}function pk(n,t,e,r,i,o){let s=fn(n,t);gk(t[Se],s,o,n.value,e,r,i)}function gk(n,t,e,r,i,o,s){if(o==null)n.removeAttribute(t,i,e);else{let a=s==null?ds(o):s(o,r||"",i);n.setAttribute(t,i,a,e)}}function bk(n,t,e,r,i,o){let s=o[t];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Jf(r,e,c,l)}}function Wm(n,t,e,r,i){let o=Ve+e,s=t[L],a=i(s,t,n,r,e);t[o]=a,gi(n,!0);let c=n.type===2;return c?(l_(t[Se],a,n),(Cb()===0||ho(n))&&Do(a,t),Eb()):Do(a,t),Vc()&&(!c||!cl(n))&&Um(s,t,a,n),n}function qm(n){let t=n;return pf()?gf():(t=t.parent,gi(t,!1)),t}function yk(n,t){let e=n[Qn];if(!e)return;let r;try{r=e.get(tr,null)}catch{r=null}r?.(t)}function Ym(n,t,e,r,i){let o=n.inputs?.[r],s=n.hostDirectiveInputs?.[r],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],u=t.data[l];Jf(u,e[l],d,i),a=!0}if(o)for(let c of o){let l=e[c],d=t.data[c];Jf(d,l,r,i),a=!0}return a}function _k(n,t){let e=Wt(t,n),r=e[L];vk(r,e);let i=e[ln];i!==null&&e[li]===null&&(e[li]=Xy(i,e[Qn])),Ee(ve.ComponentStart);try{Zm(r,e,e[Ye])}finally{Ee(ve.ComponentEnd,e[Ye])}}function vk(n,t){for(let e=t.length;e<n.blueprint.length;e++)t.push(n.blueprint[e])}function Zm(n,t,e){Lc(t);try{let r=n.viewQuery;r!==null&&$f(1,r,e);let i=n.template;i!==null&&w_(n,t,i,1,e),n.firstCreatePass&&(n.firstCreatePass=!1),t[Nn]?.finishViewCreation(n),n.staticContentQueries&&Jy(n,t),n.staticViewQueries&&$f(2,n.viewQuery,e);let o=n.components;o!==null&&xk(t,o)}catch(r){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),r}finally{t[$]&=-5,Bc()}}function xk(n,t){for(let e=0;e<t.length;e++)_k(n,t[e])}function Ps(n,t,e,r){let i=N(null);try{let o=t.tView,a=n[$]&4096?4096:16,c=Lm(n,o,e,a,null,t,null,null,r?.injector??null,r?.embeddedViewInjector??null,r?.dehydratedView??null),l=n[t.index];c[Ir]=l;let d=n[Nn];return d!==null&&(c[Nn]=d.createEmbeddedView(o)),Zm(o,c,e),c}finally{N(i)}}function wo(n,t){return!t||t.firstChild===null||zy(n)}function Es(n,t,e,r,i=!1){for(;e!==null;){if(e.type===128){e=i?e.projectionNext:e.next;continue}let o=t[e.index];o!==null&&r.push(Gt(o)),un(o)&&k_(o,r);let s=e.type;if(s&8)Es(n,t,e.child,r);else if(s&32){let a=Vm(e,t),c;for(;c=a();)r.push(c)}else if(s&16){let a=x_(t,e);if(Array.isArray(a))r.push(...a);else{let c=Dr(t[kt]);Es(c[L],c,a,r,!0)}}e=i?e.projectionNext:e.next}return r}function k_(n,t){for(let e=ze;e<n.length;e++){let r=n[e],i=r[L].firstChild;i!==null&&Es(r[L],r,i,t)}n[Tr]!==n[ln]&&t.push(n[Tr])}function I_(n){if(n[fi]!==null){for(let t of n[fi])t.impl.addSequence(t);n[fi].length=0}}var S_=[];function Dk(n){return n[Rt]??wk(n)}function wk(n){let t=S_.pop()??Object.create(Ek);return t.lView=n,t}function Ck(n){n.lView[Rt]!==n&&(n.lView=null,S_.push(n))}var Ek=W(w({},gr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{pi(n.lView)},consumerOnSignalRead(){this.lView[Rt]=this}});function kk(n){let t=n[Rt]??Object.create(Ik);return t.lView=n,t}var Ik=W(w({},gr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let t=Dr(n.lView);for(;t&&!T_(t[L]);)t=Dr(t);t&&sf(t)},consumerOnSignalRead(){this.lView[Rt]=this}});function T_(n){return n.type!==2}function M_(n){if(n[xr]===null)return;let t=!0;for(;t;){let e=!1;for(let r of n[xr])r.dirty&&(e=!0,r.zone===null||Zone.current===r.zone?r.run():r.zone.run(()=>r.run()));t=e&&!!(n[$]&8192)}}var Sk=100;function A_(n,t=0){let r=n[dn].rendererFactory,i=!1;i||r.begin?.();try{Tk(n,t)}finally{i||r.end?.()}}function Tk(n,t){let e=yf();try{as(!0),nm(n,t);let r=0;for(;hs(n);){if(r===Sk)throw new M(103,!1);r++,nm(n,1)}}finally{as(e)}}function Mk(n,t,e,r){if(hi(t))return;let i=t[$],o=!1,s=!1;Lc(t);let a=!0,c=null,l=null;o||(T_(n)?(l=Dk(t),c=Yn(l)):Na()===null?(a=!1,l=kk(t),c=Yn(l)):t[Rt]&&(_r(t[Rt]),t[Rt]=null));try{of(t),Ib(n.bindingStartIndex),e!==null&&w_(n,t,e,2,r);let d=(i&3)===3;if(!o)if(d){let h=n.preOrderCheckHooks;h!==null&&Gc(t,h,null)}else{let h=n.preOrderHooks;h!==null&&Wc(t,h,0,null),Sf(t,0)}if(s||Ak(t),M_(t),R_(t,0),n.contentQueries!==null&&Jy(n,t),!o)if(d){let h=n.contentCheckHooks;h!==null&&Gc(t,h)}else{let h=n.contentHooks;h!==null&&Wc(t,h,1),Sf(t,1)}Ok(n,t);let u=n.components;u!==null&&N_(t,u,0);let m=n.viewQuery;if(m!==null&&$f(2,m,r),!o)if(d){let h=n.viewCheckHooks;h!==null&&Gc(t,h)}else{let h=n.viewHooks;h!==null&&Wc(t,h,2),Sf(t,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),t[Tc]){for(let h of t[Tc])h();t[Tc]=null}o||(I_(t),t[$]&=-73)}catch(d){throw o||pi(t),d}finally{l!==null&&(yr(l,c),a&&Ck(l)),Bc()}}function R_(n,t){for(let e=$y(n);e!==null;e=Gy(e))for(let r=ze;r<e.length;r++){let i=e[r];O_(i,t)}}function Ak(n){for(let t=$y(n);t!==null;t=Gy(t)){if(!(t[$]&2))continue;let e=t[mi];for(let r=0;r<e.length;r++){let i=e[r];sf(i)}}}function Rk(n,t,e){Ee(ve.ComponentStart);let r=Wt(t,n);try{O_(r,e)}finally{Ee(ve.ComponentEnd,r[Ye])}}function O_(n,t){Rc(n)&&nm(n,t)}function nm(n,t){let r=n[L],i=n[$],o=n[Rt],s=!!(t===0&&i&16);if(s||=!!(i&64&&t===0),s||=!!(i&1024),s||=!!(o?.dirty&&Qi(o)),s||=!1,o&&(o.dirty=!1),n[$]&=-9217,s)Mk(r,n,r.template,n[Ye]);else if(i&8192){let a=N(null);try{M_(n),R_(n,1);let c=r.components;c!==null&&N_(n,c,1),I_(n)}finally{N(a)}}}function N_(n,t,e){for(let r=0;r<t.length;r++)Rk(n,t[r],e)}function Ok(n,t){let e=n.hostBindingOpCodes;if(e!==null)try{for(let r=0;r<e.length;r++){let i=e[r];if(i<0)Ar(~i);else{let o=i,s=e[++r],a=e[++r];Tb(s,o);let c=t[o];Ee(ve.HostBindingsUpdateStart,c);try{a(2,c)}finally{Ee(ve.HostBindingsUpdateEnd,c)}}}}finally{Ar(-1)}}function Km(n,t){let e=yf()?64:1088;for(n[dn].changeDetectionScheduler?.notify(t);n;){n[$]|=e;let r=Dr(n);if(po(n)&&!r)return n;n=r}return null}function P_(n,t,e,r){return[n,!0,0,t,null,r,null,e,null,null]}function F_(n,t){let e=ze+t;if(e<n.length)return n[e]}function Fs(n,t,e,r=!0){let i=t[L];if(Nk(i,t,n,e),r){let s=tm(e,n),a=t[Se],c=a.parentNode(n[Tr]);c!==null&&KE(i,n[Et],a,t,c,s)}let o=t[li];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function L_(n,t){let e=ks(n,t);return e!==void 0&&pl(e[L],e),e}function ks(n,t){if(n.length<=ze)return;let e=ze+t,r=n[e];if(r){let i=r[Ir];i!==null&&i!==n&&zm(i,r),t>0&&(n[e-1][$t]=r[$t]);let o=us(n,ze+t);ZE(r[L],r);let s=o[Nn];s!==null&&s.detachView(o[L]),r[rt]=null,r[$t]=null,r[$]&=-129}return r}function Nk(n,t,e,r){let i=ze+r,o=e.length;r>0&&(e[i-1][$t]=t),r<o-ze?(t[$t]=e[i],Gu(e,ze+r,t)):(e.push(t),t[$t]=null),t[rt]=e;let s=t[Ir];s!==null&&e!==s&&B_(s,t);let a=t[Nn];a!==null&&a.insertView(n),Oc(t),t[$]|=128}function B_(n,t){let e=n[mi],r=t[rt];if(Jn(r))n[$]|=2;else{let i=r[rt][kt];t[kt]!==i&&(n[$]|=2)}e===null?n[mi]=[t]:e.push(t)}var Rr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let t=this._lView,e=t[L];return Es(e,t,e.firstChild,[])}constructor(t,e){this._lView=t,this._cdRefInjectingView=e}get context(){return this._lView[Ye]}set context(t){this._lView[Ye]=t}get destroyed(){return hi(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let t=this._lView[rt];if(un(t)){let e=t[ms],r=e?e.indexOf(this):-1;r>-1&&(ks(t,r),us(e,r))}this._attachedToViewContainer=!1}pl(this._lView[L],this._lView)}onDestroy(t){af(this._lView,t)}markForCheck(){Km(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[$]&=-129}reattach(){Oc(this._lView),this._lView[$]|=128}detectChanges(){this._lView[$]|=1024,A_(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new M(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let t=po(this._lView),e=this._lView[Ir];e!==null&&!t&&zm(e,this._lView),y_(this._lView[L],this._lView)}attachToAppRef(t){if(this._attachedToViewContainer)throw new M(902,!1);this._appRef=t;let e=po(this._lView),r=this._lView[Ir];r!==null&&!e&&B_(r,this._lView),Oc(this._lView)}};var yt=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=Pk;constructor(e,r,i){this._declarationLView=e,this._declarationTContainer=r,this.elementRef=i}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,r){return this.createEmbeddedViewImpl(e,r)}createEmbeddedViewImpl(e,r,i){let o=Ps(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:r,dehydratedView:i});return new Rr(o)}}return n})();function Pk(){return yl(at(),H())}function yl(n,t){return n.type&4?new yt(t,n,ko(n,t)):null}function ki(n,t,e,r,i){let o=n.data[t];if(o===null)o=Fk(n,t,e,r,i),Sb()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=r,o.attrs=i;let s=kb();o.injectorIndex=s===null?-1:s.injectorIndex}return gi(o,!0),o}function Fk(n,t,e,r,i){let o=hf(),s=pf(),a=s?o:o&&o.parent,c=n.data[t]=Bk(n,a,e,t,r,i);return Lk(n,c,o,s),c}function Lk(n,t,e,r){n.firstChild===null&&(n.firstChild=t),e!==null&&(r?e.child==null&&t.parent!==null&&(e.child=t):e.next===null&&(e.next=t,t.prev=e))}function Bk(n,t,e,r,i,o){let s=t?t.injectorIndex:-1,a=0;return uf()&&(a|=128),{type:e,index:r,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:i,attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:t,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function Vk(n){let t=n[Ju]??[],r=n[rt][Se],i=[];for(let o of t)o.data[Zy]!==void 0?i.push(o):jk(o,r);n[Ju]=i}function jk(n,t){let e=0,r=n.firstChild;if(r){let i=n.data[Yy];for(;e<i;){let o=r.nextSibling;c_(t,r,!1),r=o,e++}}}var Hk=()=>null,zk=()=>null;function el(n,t){return Hk(n,t)}function V_(n,t,e){return zk(n,t,e)}var j_=class{},_l=class{},rm=class{resolveComponentFactory(t){throw new M(917,!1)}},vl=class{static NULL=new rm},ct=class{},lt=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>Uk()}return n})();function Uk(){let n=H(),t=at(),e=Wt(t.index,n);return(Jn(e)?e:n)[Se]}var H_=(()=>{class n{static \u0275prov=C({token:n,providedIn:"root",factory:()=>null})}return n})();var Yc={},im=class{injector;parentInjector;constructor(t,e){this.injector=t,this.parentInjector=e}get(t,e,r){let i=this.injector.get(t,Yc,r);return i!==Yc||e===Yc?i:this.parentInjector.get(t,e,r)}};function tl(n,t,e){let r=e?n.styles:null,i=e?n.classes:null,o=0;if(t!==null)for(let s=0;s<t.length;s++){let a=t[s];if(typeof a=="number")o=a;else if(o==1)i=Dc(i,a);else if(o==2){let c=a,l=t[++s];r=Dc(r,c+": "+l+";")}}e?n.styles=r:n.stylesWithoutHost=r,e?n.classes=i:n.classesWithoutHost=i}function Ne(n,t=0){let e=H();if(e===null)return K(n,t);let r=at();return By(r,e,ft(n),t)}function z_(n,t,e,r,i){let o=r===null?null:{"":-1},s=i(n,e);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}Wk(n,t,e,a,o,c,l)}o!==null&&r!==null&&$k(e,r,o)}function $k(n,t,e){let r=n.localNames=[];for(let i=0;i<t.length;i+=2){let o=e[t[i+1]];if(o==null)throw new M(-301,!1);r.push(t[i],o)}}function Gk(n,t,e){t.componentOffset=e,(n.components??=[]).push(t.index)}function Wk(n,t,e,r,i,o,s){let a=r.length,c=null;for(let m=0;m<a;m++){let h=r[m];c===null&&Pn(h)&&(c=h,Gk(n,e,m)),jf(Xc(e,t),n,h.type)}Xk(e,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let m=0;m<a;m++){let h=r[m];h.providersResolver&&h.providersResolver(h)}let l=!1,d=!1,u=h_(n,t,a,null);a>0&&(e.directiveToIndex=new Map);for(let m=0;m<a;m++){let h=r[m];if(e.mergedAttrs=xo(e.mergedAttrs,h.hostAttrs),Yk(n,e,t,u,h),Qk(u,h,i),s!==null&&s.has(h)){let[g,y]=s.get(h);e.directiveToIndex.set(h.type,[u,g+e.directiveStart,y+e.directiveStart])}else(o===null||!o.has(h))&&e.directiveToIndex.set(h.type,u);h.contentQueries!==null&&(e.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(e.flags|=64);let p=h.type.prototype;!l&&(p.ngOnChanges||p.ngOnInit||p.ngDoCheck)&&((n.preOrderHooks??=[]).push(e.index),l=!0),!d&&(p.ngOnChanges||p.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(e.index),d=!0),u++}qk(n,e,o)}function qk(n,t,e){for(let r=t.directiveStart;r<t.directiveEnd;r++){let i=n.data[r];if(e===null||!e.has(i))uy(0,t,i,r),uy(1,t,i,r),my(t,r,!1);else{let o=e.get(i);fy(0,t,o,r),fy(1,t,o,r),my(t,r,!0)}}}function uy(n,t,e,r){let i=n===0?e.inputs:e.outputs;for(let o in i)if(i.hasOwnProperty(o)){let s;n===0?s=t.inputs??={}:s=t.outputs??={},s[o]??=[],s[o].push(r),U_(t,o)}}function fy(n,t,e,r){let i=n===0?e.inputs:e.outputs;for(let o in i)if(i.hasOwnProperty(o)){let s=i[o],a;n===0?a=t.hostDirectiveInputs??={}:a=t.hostDirectiveOutputs??={},a[s]??=[],a[s].push(r,o),U_(t,s)}}function U_(n,t){t==="class"?n.flags|=8:t==="style"&&(n.flags|=16)}function my(n,t,e){let{attrs:r,inputs:i,hostDirectiveInputs:o}=n;if(r===null||!e&&i===null||e&&o===null||Pm(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let s=null,a=0;for(;a<r.length;){let c=r[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&i.hasOwnProperty(c)){let l=i[c];for(let d of l)if(d===t){s??=[],s.push(c,r[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===t){s??=[],s.push(l[d+1],r[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(s)}function Yk(n,t,e,r,i){n.data[r]=i;let o=i.factory||(i.factory=ii(i.type,!0)),s=new _i(o,Pn(i),Ne,null);n.blueprint[r]=s,e[r]=s,Zk(n,t,r,h_(n,e,i.hostVars,Pt),i)}function Zk(n,t,e,r,i){let o=i.hostBindings;if(o){let s=n.hostBindingOpCodes;s===null&&(s=n.hostBindingOpCodes=[]);let a=~t.index;Kk(s)!=a&&s.push(a),s.push(e,r,o)}}function Kk(n){let t=n.length;for(;t>0;){let e=n[--t];if(typeof e=="number"&&e<0)return e}return 0}function Qk(n,t,e){if(e){if(t.exportAs)for(let r=0;r<t.exportAs.length;r++)e[t.exportAs[r]]=n;Pn(t)&&(e[""]=n)}}function Xk(n,t,e){n.flags|=1,n.directiveStart=t,n.directiveEnd=t+e,n.providerIndexes=t}function Qm(n,t,e,r,i,o,s,a){let c=t[L],l=c.consts,d=qt(l,s),u=ki(c,n,e,r,d);return o&&z_(c,t,u,qt(l,a),i),u.mergedAttrs=xo(u.mergedAttrs,u.attrs),u.attrs!==null&&tl(u,u.attrs,!1),u.mergedAttrs!==null&&tl(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function Xm(n,t){Ty(n,t),ef(t)&&n.queries.elementEnd(t)}function Jk(n,t,e,r,i,o){let s=t.consts,a=qt(s,i),c=ki(t,n,e,r,a);if(c.mergedAttrs=xo(c.mergedAttrs,c.attrs),o!=null){let l=qt(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&tl(c,c.attrs,!1),c.mergedAttrs!==null&&tl(c,c.mergedAttrs,!0),t.queries!==null&&t.queries.elementStart(t,c),c}function $_(n,t,e){return n[t]=e}function eI(n,t){return n[t]}function Vn(n,t,e){if(e===Pt)return!1;let r=n[t];return Object.is(r,e)?!1:(n[t]=e,!0)}function Af(n,t,e){return function r(i){let o=r.__ngNativeEl__;o!==void 0&&KC(i,o);let s=er(n)?Wt(n.index,t):t;Km(s,5);let a=t[Ye],c=hy(t,a,e,i),l=r.__ngNextListenerFn__;for(;l;)c=hy(t,a,l,i)&&c,l=l.__ngNextListenerFn__;return c}}function hy(n,t,e,r){let i=N(null);try{return Ee(ve.OutputStart,t,e),e(r)!==!1}catch(o){return yk(n,o),!1}finally{Ee(ve.OutputEnd,t,e),N(i)}}function tI(n,t,e,r,i,o,s,a){let c=ho(n),l=!1,d=null;if(!r&&c&&(d=rI(t,e,o,n.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let u=fn(n,e),m=r?r(u):u;XC(e,m,o,a),r||(a.__ngNativeEl__=u);let h=i.listen(m,o,a);if(!nI(o)){let p=r?g=>r(Gt(g[n.index])):n.index;G_(p,t,e,o,a,h,!1)}}return l}function nI(n){return n.startsWith("animation")||n.startsWith("transition")}function rI(n,t,e,r){let i=n.cleanup;if(i!=null)for(let o=0;o<i.length-1;o+=2){let s=i[o];if(s===e&&i[o+1]===r){let a=t[fo],c=i[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function G_(n,t,e,r,i,o,s){let a=t.firstCreatePass?lf(t):null,c=cf(e),l=c.length;c.push(i,o),a&&a.push(r,n,l,(l+1)*(s?-1:1))}function py(n,t,e,r,i,o){let s=t[e],a=t[L],l=a.data[e].outputs[r],u=s[l].subscribe(o);G_(n.index,a,t,i,o,u,!0)}var om=Symbol("BINDING");function W_(n){return n.debugInfo?.className||n.type.name||null}var sm=class extends vl{ngModule;constructor(t){super(),this.ngModule=t}resolveComponentFactory(t){let e=Er(t);return new Co(e,this.ngModule)}};function iI(n){return Object.keys(n).map(t=>{let[e,r,i]=n[t],o={propName:e,templateName:t,isSignal:(r&fl.SignalBased)!==0};return i&&(o.transform=i),o})}function oI(n){return Object.keys(n).map(t=>({propName:n[t],templateName:t}))}function sI(n,t,e){let r=t instanceof qe?t:t?.injector;return r&&n.getStandaloneInjector!==null&&(r=n.getStandaloneInjector(r)||r),r?new im(e,r):e}function aI(n){let t=n.get(ct,null);if(t===null)throw new M(407,!1);let e=n.get(H_,null),r=n.get(On,null),i=n.get(yn,null,{optional:!0});return{rendererFactory:t,sanitizer:e,changeDetectionScheduler:r,ngReflect:!1,tracingService:i}}function cI(n,t){let e=q_(n);return s_(t,e,e==="svg"?tf:e==="math"?yb:null)}function q_(n){return(n.selectors[0][0]||"div").toLowerCase()}var Co=class extends _l{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=iI(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=oI(this.componentDef.outputs),this.cachedOutputs}constructor(t,e){super(),this.componentDef=t,this.ngModule=e,this.componentType=t.type,this.selector=LE(t.selectors),this.ngContentSelectors=t.ngContentSelectors??[],this.isBoundToModule=!!e}create(t,e,r,i,o,s){Ee(ve.DynamicComponentStart);let a=N(null);try{let c=this.componentDef,l=sI(c,i||this.ngModule,t),d=aI(l),u=d.tracingService;return u&&u.componentCreate?u.componentCreate(W_(c),()=>this.createComponentRef(d,l,e,r,o,s)):this.createComponentRef(d,l,e,r,o,s)}finally{N(a)}}createComponentRef(t,e,r,i,o,s){let a=this.componentDef,c=lI(i,a,s,o),l=t.rendererFactory.createRenderer(null,a),d=i?ak(l,i,a.encapsulation,e):cI(a,l),u=s?.some(gy)||o?.some(p=>typeof p!="function"&&p.bindings.some(gy)),m=Lm(null,c,null,512|m_(a),null,null,t,l,e,null,Xy(d,e,!0));m[Ve]=d,Lc(m);let h=null;try{let p=Qm(Ve,m,2,"#host",()=>c.directiveRegistry,!0,0);l_(l,d,p),Do(d,m),bl(c,m,p),Sm(c,p,m),Xm(c,p),r!==void 0&&uI(p,this.ngContentSelectors,r),h=Wt(p.index,m),m[Ye]=h[Ye],Zm(c,m,null)}catch(p){throw h!==null&&zf(h),zf(m),p}finally{Ee(ve.DynamicComponentEnd),Bc()}return new nl(this.componentType,m,!!u)}};function lI(n,t,e,r){let i=n?["ng-version","21.2.12"]:BE(t.selectors[0]),o=null,s=null,a=0;if(e)for(let d of e)a+=d[om].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(r)for(let d=0;d<r.length;d++){let u=r[d];if(typeof u!="function")for(let m of u.bindings){a+=m[om].requiredVars;let h=d+1;m.create&&(m.targetIdx=h,(o??=[]).push(m)),m.update&&(m.targetIdx=h,(s??=[]).push(m))}}let c=[t];if(r)for(let d of r){let u=typeof d=="function"?d:d.type,m=Hu(u);c.push(m)}return Fm(0,null,dI(o,s),1,a,c,null,null,null,[i],null)}function dI(n,t){return!n&&!t?null:e=>{if(e&1&&n)for(let r of n)r.create();if(e&2&&t)for(let r of t)r.update()}}function gy(n){let t=n[om].kind;return t==="input"||t==="twoWay"}var nl=class extends j_{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(t,e,r){super(),this._rootLView=e,this._hasInputBindings=r,this._tNode=Mc(e[L],Ve),this.location=ko(this._tNode,e),this.instance=Wt(this._tNode.index,e)[Ye],this.hostView=this.changeDetectorRef=new Rr(e,void 0),this.componentType=t}setInput(t,e){this._hasInputBindings;let r=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(t)&&Object.is(this.previousInputValues.get(t),e))return;let i=this._rootLView,o=Ym(r,i[L],i,t,e);this.previousInputValues.set(t,e);let s=Wt(r.index,i);Km(s,1)}get injector(){return new yi(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(t){this.hostView.onDestroy(t)}};function uI(n,t,e){let r=n.projection=[];for(let i=0;i<t.length;i++){let o=e[i];r.push(o!=null&&o.length?Array.from(o):null)}}var Lt=(()=>{class n{static __NG_ELEMENT_ID__=fI}return n})();function fI(){let n=at();return Y_(n,H())}var am=class n extends Lt{_lContainer;_hostTNode;_hostLView;constructor(t,e,r){super(),this._lContainer=t,this._hostTNode=e,this._hostLView=r}get element(){return ko(this._hostTNode,this._hostLView)}get injector(){return new yi(this._hostTNode,this._hostLView)}get parentInjector(){let t=wm(this._hostTNode,this._hostLView);if(Ry(t)){let e=Qc(t,this._hostLView),r=Kc(t),i=e[L].data[r+8];return new yi(i,e)}else return new yi(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(t){let e=by(this._lContainer);return e!==null&&e[t]||null}get length(){return this._lContainer.length-ze}createEmbeddedView(t,e,r){let i,o;typeof r=="number"?i=r:r!=null&&(i=r.index,o=r.injector);let s=el(this._lContainer,t.ssrId),a=t.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,i,wo(this._hostTNode,s)),a}createComponent(t,e,r,i,o,s,a){let c=t&&!CC(t),l;if(c)l=e;else{let y=e||{};l=y.index,r=y.injector,i=y.projectableNodes,o=y.environmentInjector||y.ngModuleRef,s=y.directives,a=y.bindings}let d=c?t:new Co(Er(t)),u=r||this.parentInjector;if(!o&&d.ngModule==null){let D=(c?u:this.parentInjector).get(qe,null);D&&(o=D)}let m=Er(d.componentType??{}),h=el(this._lContainer,m?.id??null),p=h?.firstChild??null,g=d.create(u,i,p,o,s,a);return this.insertImpl(g.hostView,l,wo(this._hostTNode,h)),g}insert(t,e){return this.insertImpl(t,e,!0)}insertImpl(t,e,r){let i=t._lView;if(vb(i)){let a=this.indexOf(t);if(a!==-1)this.detach(a);else{let c=i[rt],l=new n(c,c[Et],c[rt]);l.detach(l.indexOf(t))}}let o=this._adjustIndex(e),s=this._lContainer;return Fs(s,i,o,r),t.attachToViewContainerRef(),Gu(Rf(s),o,t),t}move(t,e){return this.insert(t,e)}indexOf(t){let e=by(this._lContainer);return e!==null?e.indexOf(t):-1}remove(t){let e=this._adjustIndex(t,-1),r=ks(this._lContainer,e);r&&(us(Rf(this._lContainer),e),pl(r[L],r))}detach(t){let e=this._adjustIndex(t,-1),r=ks(this._lContainer,e);return r&&us(Rf(this._lContainer),e)!=null?new Rr(r):null}_adjustIndex(t,e=0){return t??this.length+e}};function by(n){return n[ms]}function Rf(n){return n[ms]||(n[ms]=[])}function Y_(n,t){let e,r=t[n.index];return un(r)?e=r:(e=P_(r,t,null,n),t[n.index]=e,Bm(t,e)),hI(e,t,n,r),new am(e,n,t)}function mI(n,t){let e=n[Se],r=e.createComment(""),i=fn(t,n),o=e.parentNode(i);return Jc(e,o,r,e.nextSibling(i),!1),r}var hI=bI,pI=()=>!1;function gI(n,t,e){return pI(n,t,e)}function bI(n,t,e,r){if(n[Tr])return;let i;e.type&8?i=Gt(r):i=mI(t,e),n[Tr]=i}var cm=class n{queryList;matches=null;constructor(t){this.queryList=t}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},lm=class n{queries;constructor(t=[]){this.queries=t}createEmbeddedView(t){let e=t.queries;if(e!==null){let r=t.contentQueries!==null?t.contentQueries[0]:e.length,i=[];for(let o=0;o<r;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];i.push(a.clone())}return new n(i)}return null}insertView(t){this.dirtyQueriesWithMatches(t)}detachView(t){this.dirtyQueriesWithMatches(t)}finishViewCreation(t){this.dirtyQueriesWithMatches(t)}dirtyQueriesWithMatches(t){for(let e=0;e<this.queries.length;e++)eh(t,e).matches!==null&&this.queries[e].setDirty()}},rl=class{flags;read;predicate;constructor(t,e,r=null){this.flags=e,this.read=r,typeof t=="string"?this.predicate=DI(t):this.predicate=t}},dm=class n{queries;constructor(t=[]){this.queries=t}elementStart(t,e){for(let r=0;r<this.queries.length;r++)this.queries[r].elementStart(t,e)}elementEnd(t){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(t)}embeddedTView(t){let e=null;for(let r=0;r<this.length;r++){let i=e!==null?e.length:0,o=this.getByIndex(r).embeddedTView(t,i);o&&(o.indexInDeclarationView=r,e!==null?e.push(o):e=[o])}return e!==null?new n(e):null}template(t,e){for(let r=0;r<this.queries.length;r++)this.queries[r].template(t,e)}getByIndex(t){return this.queries[t]}get length(){return this.queries.length}track(t){this.queries.push(t)}},um=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(t,e=-1){this.metadata=t,this._declarationNodeIndex=e}elementStart(t,e){this.isApplyingToNode(e)&&this.matchTNode(t,e)}elementEnd(t){this._declarationNodeIndex===t.index&&(this._appliesToNextNode=!1)}template(t,e){this.elementStart(t,e)}embeddedTView(t,e){return this.isApplyingToNode(t)?(this.crossesNgTemplate=!0,this.addMatch(-t.index,e),new n(this.metadata)):null}isApplyingToNode(t){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,r=t.parent;for(;r!==null&&r.type&8&&r.index!==e;)r=r.parent;return e===(r!==null?r.index:-1)}return this._appliesToNextNode}matchTNode(t,e){let r=this.metadata.predicate;if(Array.isArray(r))for(let i=0;i<r.length;i++){let o=r[i];this.matchTNodeWithReadOption(t,e,yI(e,o)),this.matchTNodeWithReadOption(t,e,qc(e,t,o,!1,!1))}else r===yt?e.type&4&&this.matchTNodeWithReadOption(t,e,-1):this.matchTNodeWithReadOption(t,e,qc(e,t,r,!1,!1))}matchTNodeWithReadOption(t,e,r){if(r!==null){let i=this.metadata.read;if(i!==null)if(i===z||i===Lt||i===yt&&e.type&4)this.addMatch(e.index,-2);else{let o=qc(e,t,i,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,r)}}addMatch(t,e){this.matches===null?this.matches=[t,e]:this.matches.push(t,e)}};function yI(n,t){let e=n.localNames;if(e!==null){for(let r=0;r<e.length;r+=2)if(e[r]===t)return e[r+1]}return null}function _I(n,t){return n.type&11?ko(n,t):n.type&4?yl(n,t):null}function vI(n,t,e,r){return e===-1?_I(t,n):e===-2?xI(n,t,r):ws(n,n[L],e,t)}function xI(n,t,e){if(e===z)return ko(t,n);if(e===yt)return yl(t,n);if(e===Lt)return Y_(t,n)}function Z_(n,t,e,r){let i=t[Nn].queries[r];if(i.matches===null){let o=n.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(vI(t,d,s[c+1],e.metadata.read))}}i.matches=a}return i.matches}function fm(n,t,e,r){let i=n.queries.getByIndex(e),o=i.matches;if(o!==null){let s=Z_(n,t,i,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)r.push(s[a/2]);else{let l=o[a+1],d=t[-c];for(let u=ze;u<d.length;u++){let m=d[u];m[Ir]===m[rt]&&fm(m[L],m,l,r)}if(d[mi]!==null){let u=d[mi];for(let m=0;m<u.length;m++){let h=u[m];fm(h[L],h,l,r)}}}}}return r}function Jm(n,t){return n[Nn].queries[t].queryList}function K_(n,t,e){let r=new nr((e&4)===4);return wb(n,t,r,r.destroy),(t[Nn]??=new lm).queries.push(new cm(r))-1}function Q_(n,t,e){let r=Oe();return r.firstCreatePass&&(J_(r,new rl(n,t,e),-1),(t&2)===2&&(r.staticViewQueries=!0)),K_(r,H(),t)}function X_(n,t,e,r){let i=Oe();if(i.firstCreatePass){let o=at();J_(i,new rl(t,e,r),o.index),wI(i,n),(e&2)===2&&(i.staticContentQueries=!0)}return K_(i,H(),e)}function DI(n){return n.split(",").map(t=>t.trim())}function J_(n,t,e){n.queries===null&&(n.queries=new dm),n.queries.track(new um(t,e))}function wI(n,t){let e=n.contentQueries||(n.contentQueries=[]),r=e.length?e[e.length-1]:-1;t!==r&&e.push(n.queries.length-1,t)}function eh(n,t){return n.queries.getByIndex(t)}function ev(n,t){let e=n[L],r=eh(e,t);return r.crossesNgTemplate?fm(e,n,t,[]):Z_(e,n,r,t)}function tv(n,t,e){let r,i=Ko(()=>{r._dirtyCounter();let o=CI(r,n);if(t&&o===void 0)throw new M(-951,!1);return o});return r=i[We],r._dirtyCounter=X(0),r._flatValue=void 0,i}function th(n){return tv(!0,!1,n)}function nh(n){return tv(!0,!0,n)}function nv(n,t){let e=n[We];e._lView=H(),e._queryIndex=t,e._queryList=Jm(e._lView,t),e._queryList.onDirty(()=>e._dirtyCounter.update(r=>r+1))}function CI(n,t){let e=n._lView,r=n._queryIndex;if(e===void 0||r===void 0||e[$]&4)return t?void 0:gt;let i=Jm(e,r),o=ev(e,r);return i.reset(o,Hy),t?i.first:i._changesDetected||n._flatValue===void 0?n._flatValue=i.toArray():n._flatValue}var Or=class{};var Is=class extends Or{injector;componentFactoryResolver=new sm(this);instance=null;constructor(t){super();let e=new si([...t.providers,{provide:Or,useValue:this},{provide:vl,useValue:this.componentFactoryResolver}],t.parent||lo(),t.debugName,new Set(["environment"]));this.injector=e,t.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(t){this.injector.onDestroy(t)}};function rv(n,t,e=null){return new Is({providers:n,parent:t,debugName:e,runEnvironmentInitializers:!0}).injector}var EI=(()=>{class n{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let r=Yu(!1,e.type),i=r.length>0?rv([r],this._injector,""):null;this.cachedInjectors.set(e,i)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=C({token:n,providedIn:"environment",factory:()=>new n(K(qe))})}return n})();function q(n){return Ms(()=>{let t=iv(n),e=W(w({},t),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===Em.OnPush,directiveDefs:null,pipeDefs:null,dependencies:t.standalone&&n.dependencies||null,getStandaloneInjector:t.standalone?i=>i.get(EI).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||gn.Emulated,styles:n.styles||gt,_:null,schemas:n.schemas||null,tView:null,id:""});t.standalone&&sr("NgStandalone"),ov(e);let r=n.dependencies;return e.directiveDefs=yy(r,kI),e.pipeDefs=yy(r,rb),e.id=TI(e),e})}function kI(n){return Er(n)||Hu(n)}function je(n){return Ms(()=>({type:n.type,bootstrap:n.bootstrap||gt,declarations:n.declarations||gt,imports:n.imports||gt,exports:n.exports||gt,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function II(n,t){if(n==null)return kr;let e={};for(let r in n)if(n.hasOwnProperty(r)){let i=n[r],o,s,a,c;Array.isArray(i)?(a=i[0],o=i[1],s=i[2]??o,c=i[3]||null):(o=i,s=i,a=fl.None,c=null),e[o]=[r,a,c],t[o]=s}return e}function SI(n){if(n==null)return kr;let t={};for(let e in n)n.hasOwnProperty(e)&&(t[n[e]]=e);return t}function Y(n){return Ms(()=>{let t=iv(n);return ov(t),t})}function iv(n){let t={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:t,inputConfig:n.inputs||kr,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||gt,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:II(n.inputs,t),outputs:SI(n.outputs),debugInfo:null}}function ov(n){n.features?.forEach(t=>t(n))}function yy(n,t){return n?()=>{let e=typeof n=="function"?n():n,r=[];for(let i of e){let o=t(i);o!==null&&r.push(o)}return r}:null}function TI(n){let t=0,e=typeof n.consts=="function"?"":n.consts,r=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,e,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let o of r.join("|"))t=Math.imul(31,t)+o.charCodeAt(0)<<0;return t+=2147483648,"c"+t}function MI(n){return Object.getPrototypeOf(n.prototype).constructor}function Pe(n){let t=MI(n.type),e=!0,r=[n];for(;t;){let i;if(Pn(n))i=t.\u0275cmp||t.\u0275dir;else{if(t.\u0275cmp)throw new M(903,!1);i=t.\u0275dir}if(i){if(e){r.push(i);let s=n;s.inputs=Of(n.inputs),s.declaredInputs=Of(n.declaredInputs),s.outputs=Of(n.outputs);let a=i.hostBindings;a&&PI(n,a);let c=i.viewQuery,l=i.contentQueries;if(c&&OI(n,c),l&&NI(n,l),AI(n,i),nb(n.outputs,i.outputs),Pn(i)&&i.data.animation){let d=n.data;d.animation=(d.animation||[]).concat(i.data.animation)}}let o=i.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(n),a===Pe&&(e=!1)}}t=Object.getPrototypeOf(t)}RI(r)}function AI(n,t){for(let e in t.inputs){if(!t.inputs.hasOwnProperty(e)||n.inputs.hasOwnProperty(e))continue;let r=t.inputs[e];r!==void 0&&(n.inputs[e]=r,n.declaredInputs[e]=t.declaredInputs[e])}}function RI(n){let t=0,e=null;for(let r=n.length-1;r>=0;r--){let i=n[r];i.hostVars=t+=i.hostVars,i.hostAttrs=xo(i.hostAttrs,e=xo(e,i.hostAttrs))}}function Of(n){return n===kr?{}:n===gt?[]:n}function OI(n,t){let e=n.viewQuery;e?n.viewQuery=(r,i)=>{t(r,i),e(r,i)}:n.viewQuery=t}function NI(n,t){let e=n.contentQueries;e?n.contentQueries=(r,i,o)=>{t(r,i,o),e(r,i,o)}:n.contentQueries=t}function PI(n,t){let e=n.hostBindings;e?n.hostBindings=(r,i)=>{t(r,i),e(r,i)}:n.hostBindings=t}function sv(n,t,e,r,i,o,s,a){if(e.firstCreatePass){n.mergedAttrs=xo(n.mergedAttrs,n.attrs);let d=n.tView=Fm(2,n,i,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,n),d.queries=e.queries.embeddedTView(n))}a&&(n.flags|=a),gi(n,!1);let c=LI(e,t,n,r);Vc()&&Um(e,t,c,n),Do(c,t);let l=P_(c,t,c,n);t[r+Ve]=l,Bm(t,l),gI(l,n,t)}function FI(n,t,e,r,i,o,s,a,c,l,d){let u=e+Ve,m;return t.firstCreatePass?(m=ki(t,u,4,s||null,a||null),Nc()&&z_(t,n,m,qt(t.consts,l),Gm),Ty(t,m)):m=t.data[u],sv(m,n,t,e,r,i,o,c),ho(m)&&bl(t,n,m),l!=null&&Ns(n,m,d),m}function Ss(n,t,e,r,i,o,s,a,c,l,d){let u=e+Ve,m;if(t.firstCreatePass){if(m=ki(t,u,4,s||null,a||null),l!=null){let h=qt(t.consts,l);m.localNames=[];for(let p=0;p<h.length;p+=2)m.localNames.push(h[p],-1)}}else m=t.data[u];return sv(m,n,t,e,r,i,o,c),l!=null&&Ns(n,m,d),m}function Ue(n,t,e,r,i,o,s,a){let c=H(),l=Oe(),d=qt(l.consts,o);return FI(c,l,n,t,e,r,i,d,void 0,s,a),Ue}var LI=BI;function BI(n,t,e,r){return bs(!0),t[Se].createComment("")}function Nr(n){return typeof n=="function"&&n[We]!==void 0}function rh(n){return Nr(n)&&typeof n.set=="function"}var ih=new x("");function Io(n){return!!n&&typeof n.then=="function"}function oh(n){return!!n&&typeof n.subscribe=="function"}var av=new x("");var sh=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,r)=>{this.resolve=e,this.reject=r});appInits=f(av,{optional:!0})??[];injector=f(V);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let i of this.appInits){let o=uo(this.injector,i);if(Io(o))e.push(o);else if(oh(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let r=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{r()}).catch(i=>{this.reject(i)}),e.length===0&&r(),this.initialized=!0}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),ah=new x("");function cv(){iu(()=>{let n="";throw new M(600,n)})}function lv(n){return n.isBoundToModule}var VI=10;var Qt=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=f(tr);afterRenderManager=f(hl);zonelessEnabled=f(_s);rootEffectScheduler=f(jc);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new E;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=f(bi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Le(e=>!e))}constructor(){f(yn,{optional:!0})}whenStable(){let e;return new Promise(r=>{e=this.isStable.subscribe({next:i=>{i&&r()}})}).finally(()=>{e.unsubscribe()})}_injector=f(qe);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,r){return this.bootstrapImpl(e,r)}bootstrapImpl(e,r,i=V.NULL){return this._injector.get(F).run(()=>{Ee(ve.BootstrapComponentStart);let s=e instanceof _l;if(!this._injector.get(sh).done){let p="";throw new M(405,p)}let c;s?c=e:c=this._injector.get(vl).resolveComponentFactory(e),this.componentTypes.push(c.componentType);let l=lv(c)?void 0:this._injector.get(Or),d=r||c.selector,u=c.create(i,[],d,l),m=u.location.nativeElement,h=u.injector.get(ih,null);return h?.registerApplication(m),u.onDestroy(()=>{this.detachView(u.hostView),Ds(this.components,u),h?.unregisterApplication(m)}),this._loadComponent(u),Ee(ve.BootstrapComponentEnd,u),u})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Ee(ve.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(ml.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Ee(ve.ChangeDetectionEnd),new M(101,!1);let e=N(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,N(e),this.afterTick.next(),Ee(ve.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(ct,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<VI;){Ee(ve.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Ee(ve.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let r=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:i}of this.allViews){if(!r&&!hs(i))continue;let o=r&&!this.zonelessEnabled?0:1;A_(i,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>hs(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let r=e;this._views.push(r),r.attachToAppRef(this)}detachView(e){let r=e;Ds(this._views,r),r.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(i){this.internalErrorHandler(i)}this.components.push(e),this._injector.get(ah,[]).forEach(i=>i(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Ds(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new M(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Ds(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}function ue(n,t,e,r){let i=H(),o=Mr();if(Vn(i,o,t)){let s=Oe(),a=gs();pk(a,i,n,t,e,r)}return ue}var mm=class{destroy(t){}updateValue(t,e){}swap(t,e){let r=Math.min(t,e),i=Math.max(t,e),o=this.detach(i);if(i-r>1){let s=this.detach(r);this.attach(r,o),this.attach(i,s)}else this.attach(r,o)}move(t,e){this.attach(e,this.detach(t))}};function Nf(n,t,e,r,i){return n===e&&Object.is(t,r)?1:Object.is(i(n,t),i(e,r))?-1:0}function jI(n,t,e,r){let i,o,s=0,a=n.length-1,c=void 0;if(Array.isArray(t)){N(r);let l=t.length-1;for(N(null);s<=a&&s<=l;){let d=n.at(s),u=t[s],m=Nf(s,d,s,u,e);if(m!==0){m<0&&n.updateValue(s,u),s++;continue}let h=n.at(a),p=t[l],g=Nf(a,h,l,p,e);if(g!==0){g<0&&n.updateValue(a,p),a--,l--;continue}let y=e(s,d),D=e(a,h),R=e(s,u);if(Object.is(R,D)){let oe=e(l,p);Object.is(oe,y)?(n.swap(s,a),n.updateValue(a,p),l--,a--):n.move(a,s),n.updateValue(s,u),s++;continue}if(i??=new il,o??=vy(n,s,a,e),hm(n,i,s,R))n.updateValue(s,u),s++,a++;else if(o.has(R))i.set(y,n.detach(s)),a--;else{let oe=n.create(s,t[s]);n.attach(s,oe),s++,a++}}for(;s<=l;)_y(n,i,e,s,t[s]),s++}else if(t!=null){N(r);let l=t[Symbol.iterator]();N(null);let d=l.next();for(;!d.done&&s<=a;){let u=n.at(s),m=d.value,h=Nf(s,u,s,m,e);if(h!==0)h<0&&n.updateValue(s,m),s++,d=l.next();else{i??=new il,o??=vy(n,s,a,e);let p=e(s,m);if(hm(n,i,s,p))n.updateValue(s,m),s++,a++,d=l.next();else if(!o.has(p))n.attach(s,n.create(s,m)),s++,a++,d=l.next();else{let g=e(s,u);i.set(g,n.detach(s)),a--}}}for(;!d.done;)_y(n,i,e,n.length,d.value),d=l.next()}for(;s<=a;)n.destroy(n.detach(a--));i?.forEach(l=>{n.destroy(l)})}function hm(n,t,e,r){return t!==void 0&&t.has(r)?(n.attach(e,t.get(r)),t.delete(r),!0):!1}function _y(n,t,e,r,i){if(hm(n,t,r,e(r,i)))n.updateValue(r,i);else{let o=n.create(r,i);n.attach(r,o)}}function vy(n,t,e,r){let i=new Set;for(let o=t;o<=e;o++)i.add(r(o,n.at(o)));return i}var il=class{kvMap=new Map;_vMap=void 0;has(t){return this.kvMap.has(t)}delete(t){if(!this.has(t))return!1;let e=this.kvMap.get(t);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(t,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(t),!0}get(t){return this.kvMap.get(t)}set(t,e){if(this.kvMap.has(t)){let r=this.kvMap.get(t);this._vMap===void 0&&(this._vMap=new Map);let i=this._vMap;for(;i.has(r);)r=i.get(r);i.set(r,e)}else this.kvMap.set(t,e)}forEach(t){for(let[e,r]of this.kvMap)if(t(r,e),this._vMap!==void 0){let i=this._vMap;for(;i.has(r);)r=i.get(r),t(r,e)}}};function pe(n,t,e,r,i,o,s,a){sr("NgControlFlow");let c=H(),l=Oe(),d=qt(l.consts,o);return Ss(c,l,n,t,e,r,i,d,256,s,a),ch}function ch(n,t,e,r,i,o,s,a){sr("NgControlFlow");let c=H(),l=Oe(),d=qt(l.consts,o);return Ss(c,l,n,t,e,r,i,d,512,s,a),ch}function ge(n,t){sr("NgControlFlow");let e=H(),r=Mr(),i=e[r]!==Pt?e[r]:-1,o=i!==-1?ol(e,Ve+i):void 0,s=0;if(Vn(e,r,n)){let a=N(null);try{if(o!==void 0&&L_(o,s),n!==-1){let c=Ve+n,l=ol(e,c),d=ym(e[L],c),u=V_(l,d,e),m=Ps(e,d,t,{dehydratedView:u});Fs(l,m,s,wo(d,u))}}finally{N(a)}}else if(o!==void 0){let a=F_(o,s);a!==void 0&&(a[Ye]=t)}}var pm=class{lContainer;$implicit;$index;constructor(t,e,r){this.lContainer=t,this.$implicit=e,this.$index=r}get $count(){return this.lContainer.length-ze}};function xl(n,t){return t}var gm=class{hasEmptyBlock;trackByFn;liveCollection;constructor(t,e,r){this.hasEmptyBlock=t,this.trackByFn=e,this.liveCollection=r}};function Ii(n,t,e,r,i,o,s,a,c,l,d,u,m){sr("NgControlFlow");let h=H(),p=Oe(),g=c!==void 0,y=H(),D=a?s.bind(y[kt][Ye]):s,R=new gm(g,D);y[Ve+n]=R,Ss(h,p,n+1,t,e,r,i,qt(p.consts,o),256),g&&Ss(h,p,n+2,c,l,d,u,qt(p.consts,m),512)}var bm=class extends mm{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(t,e,r){super(),this.lContainer=t,this.hostLView=e,this.templateTNode=r}get length(){return this.lContainer.length-ze}at(t){return this.getLView(t)[Ye].$implicit}attach(t,e){let r=e[li];this.needsIndexUpdate||=t!==this.length,Fs(this.lContainer,e,t,wo(this.templateTNode,r)),HI(this.lContainer,t)}detach(t){return this.needsIndexUpdate||=t!==this.length-1,zI(this.lContainer,t),UI(this.lContainer,t)}create(t,e){let r=el(this.lContainer,this.templateTNode.tView.ssrId);return Ps(this.hostLView,this.templateTNode,new pm(this.lContainer,e,t),{dehydratedView:r})}destroy(t){pl(t[L],t)}updateValue(t,e){this.getLView(t)[Ye].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let t=0;t<this.length;t++)this.getLView(t)[Ye].$index=t}getLView(t){return $I(this.lContainer,t)}};function Si(n){let t=N(null),e=Fn();try{let r=H(),i=r[L],o=r[e],s=e+1,a=ol(r,s);if(o.liveCollection===void 0){let l=ym(i,s);o.liveCollection=new bm(a,r,l)}else o.liveCollection.reset();let c=o.liveCollection;if(jI(c,n,o.trackByFn,t),c.updateIndexes(),o.hasEmptyBlock){let l=Mr(),d=c.length===0;if(Vn(r,l,d)){let u=e+2,m=ol(r,u);if(d){let h=ym(i,u),p=V_(m,h,r),g=Ps(r,h,void 0,{dehydratedView:p});Fs(m,g,0,wo(h,p))}else i.firstUpdatePass&&Vk(m),L_(m,0)}}}finally{N(t)}}function ol(n,t){return n[t]}function HI(n,t){if(n.length<=ze)return;let e=ze+t,r=n[e],i=r?r[Sr]:void 0;if(r&&i&&i.detachedLeaveAnimationFns&&i.detachedLeaveAnimationFns.length>0){let o=r[Qn];qE(o,i),vi.delete(r[Xn]),i.detachedLeaveAnimationFns=void 0}}function zI(n,t){if(n.length<=ze)return;let e=ze+t,r=n[e],i=r?r[Sr]:void 0;i&&i.leave&&i.leave.size>0&&(i.detachedLeaveAnimationFns=[])}function UI(n,t){return ks(n,t)}function $I(n,t){return F_(n,t)}function ym(n,t){return Mc(n,t)}function te(n,t,e){let r=H(),i=Mr();if(Vn(r,i,t)){let o=Oe(),s=gs();C_(s,r,n,t,r[Se],e)}return te}function _m(n,t,e,r,i){Ym(t,n,e,i?"class":"style",r)}function I(n,t,e,r){let i=H(),o=i[L],s=n+Ve,a=o.firstCreatePass?Qm(s,i,2,t,Gm,Nc(),e,r):o.data[s];if(er(a)){let c=i[dn].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(W_(l),()=>(xy(n,t,i,a,r),I))}}return xy(n,t,i,a,r),I}function xy(n,t,e,r,i){if(Wm(r,e,n,t,dv),ho(r)){let o=e[L];bl(o,e,r),Sm(o,r,e)}i!=null&&Ns(e,r)}function T(){let n=Oe(),t=at(),e=qm(t);return n.firstCreatePass&&Xm(n,e),ff(e)&&mf(),df(),e.classesWithoutHost!=null&&MC(e)&&_m(n,e,H(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&AC(e)&&_m(n,e,H(),e.stylesWithoutHost,!1),T}function fe(n,t,e,r){return I(n,t,e,r),T(),fe}function Bt(n,t,e,r){let i=H(),o=i[L],s=n+Ve,a=o.firstCreatePass?Jk(s,o,2,t,e,r):o.data[s];return Wm(a,i,n,t,dv),r!=null&&Ns(i,a),Bt}function Xt(){let n=at(),t=qm(n);return ff(t)&&mf(),df(),Xt}function Jt(n,t,e,r){return Bt(n,t,e,r),Xt(),Jt}var dv=(n,t,e,r,i)=>(bs(!0),s_(t[Se],r,Fb()));function lh(n,t,e){let r=H(),i=r[L],o=n+Ve,s=i.firstCreatePass?Qm(o,r,8,"ng-container",Gm,Nc(),t,e):i.data[o];if(Wm(s,r,n,"ng-container",GI),ho(s)){let a=r[L];bl(a,r,s),Sm(a,s,r)}return e!=null&&Ns(r,s),lh}function dh(){let n=Oe(),t=at(),e=qm(t);return n.firstCreatePass&&Xm(n,e),dh}function ar(n,t,e){return lh(n,t,e),dh(),ar}var GI=(n,t,e,r,i)=>(bs(!0),CE(t[Se],""));function jn(){return H()}function Hn(n,t,e){let r=H(),i=Mr();if(Vn(r,i,t)){let o=Oe(),s=gs();E_(s,r,n,t,r[Se],e)}return Hn}var Ls="en-US";var WI=Ls;function uv(n){typeof n=="string"&&(WI=n.toLowerCase().replace(/_/g,"-"))}function Ce(n,t,e){let r=H(),i=Oe(),o=at();return fv(i,r,r[Se],o,n,t,e),Ce}function fv(n,t,e,r,i,o,s){let a=!0,c=null;if((r.type&3||s)&&(c??=Af(r,t,o),tI(r,n,t,s,e,i,o,c)&&(a=!1)),a){let l=r.outputs?.[i],d=r.hostDirectiveOutputs?.[i];if(d&&d.length)for(let u=0;u<d.length;u+=2){let m=d[u],h=d[u+1];c??=Af(r,t,o),py(r,t,m,h,i,c)}if(l&&l.length)for(let u of l)c??=Af(r,t,o),py(r,t,u,i,i,c)}}function ne(n=1){return Pb(n)}function qI(n,t){let e=null,r=RE(n);for(let i=0;i<t.length;i++){let o=t[i];if(o==="*"){e=i;continue}if(r===null?f_(n,o,!0):PE(r,o))return i}return e}function Ze(n){let t=H()[kt][Et];if(!t.projection){let e=n?n.length:1,r=t.projection=lb(e,null),i=r.slice(),o=t.child;for(;o!==null;){if(o.type!==128){let s=n?qI(o,n):0;s!==null&&(i[s]?i[s].projectionNext=o:r[s]=o,i[s]=o)}o=o.next}}}function ae(n,t=0,e,r,i,o){let s=H(),a=Oe(),c=r?n+1:null;c!==null&&Ss(s,a,c,r,i,o,null,e);let l=ki(a,Ve+n,16,null,e||null);l.projection===null&&(l.projection=t),gf();let u=!s[li]||uf();s[kt][Et].projection[l.projection]===null&&c!==null?YI(s,a,c):u&&!cl(l)&&ik(a,s,l)}function YI(n,t,e){let r=Ve+e,i=t.data[r],o=n[r],s=el(o,i.tView.ssrId),a=Ps(n,i,void 0,{dehydratedView:s});Fs(o,a,0,wo(i,s))}function _n(n,t,e,r){return X_(n,t,e,r),_n}function et(n,t,e){return Q_(n,t,e),et}function J(n){let t=H(),e=Oe(),r=Fc();ps(r+1);let i=eh(e,r);if(n.dirty&&_b(t)===((i.metadata.flags&2)===2)){if(i.matches===null)n.reset([]);else{let o=ev(t,r);n.reset(o,Hy),n.notifyOnChanges()}return!0}return!1}function ee(){return Jm(H(),Fc())}function Dl(n,t,e,r,i){return nv(t,X_(n,e,r,i)),Dl}function So(n,t,e,r){return nv(n,Q_(t,e,r)),So}function To(n=1){ps(Fc()+n)}function Vt(n){let t=bf();return rf(t,Ve+n)}function $c(n,t){return n<<17|t<<2}function xi(n){return n>>17&32767}function ZI(n){return(n&2)==2}function KI(n,t){return n&131071|t<<17}function vm(n){return n|2}function Eo(n){return(n&131068)>>2}function Pf(n,t){return n&-131069|t<<2}function QI(n){return(n&1)===1}function xm(n){return n|1}function XI(n,t,e,r,i,o){let s=o?t.classBindings:t.styleBindings,a=xi(s),c=Eo(s);n[r]=e;let l=!1,d;if(Array.isArray(e)){let u=e;d=u[1],(d===null||ao(u,d)>0)&&(l=!0)}else d=e;if(i)if(c!==0){let m=xi(n[a+1]);n[r+1]=$c(m,a),m!==0&&(n[m+1]=Pf(n[m+1],r)),n[a+1]=KI(n[a+1],r)}else n[r+1]=$c(a,0),a!==0&&(n[a+1]=Pf(n[a+1],r)),a=r;else n[r+1]=$c(c,0),a===0?a=r:n[c+1]=Pf(n[c+1],r),c=r;l&&(n[r+1]=vm(n[r+1])),Dy(n,d,r,!0),Dy(n,d,r,!1),JI(t,d,n,r,o),s=$c(a,c),o?t.classBindings=s:t.styleBindings=s}function JI(n,t,e,r,i){let o=i?n.residualClasses:n.residualStyles;o!=null&&typeof t=="string"&&ao(o,t)>=0&&(e[r+1]=xm(e[r+1]))}function Dy(n,t,e,r){let i=n[e+1],o=t===null,s=r?xi(i):Eo(i),a=!1;for(;s!==0&&(a===!1||o);){let c=n[s],l=n[s+1];eS(c,t)&&(a=!0,n[s+1]=r?xm(l):vm(l)),s=r?xi(l):Eo(l)}a&&(n[e+1]=r?vm(i):xm(i))}function eS(n,t){return n===null||t==null||(Array.isArray(n)?n[1]:n)===t?!0:Array.isArray(n)&&typeof t=="string"?ao(n,t)>=0:!1}var pn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function tS(n){return n.substring(pn.key,pn.keyEnd)}function nS(n){return rS(n),mv(n,hv(n,0,pn.textEnd))}function mv(n,t){let e=pn.textEnd;return e===t?-1:(t=pn.keyEnd=iS(n,pn.key=t,e),hv(n,t,e))}function rS(n){pn.key=0,pn.keyEnd=0,pn.value=0,pn.valueEnd=0,pn.textEnd=n.length}function hv(n,t,e){for(;t<e&&n.charCodeAt(t)<=32;)t++;return t}function iS(n,t,e){for(;t<e&&n.charCodeAt(t)>32;)t++;return t}function cr(n,t,e){return pv(n,t,e,!1),cr}function Q(n,t){return pv(n,t,null,!0),Q}function vn(n){sS(fS,oS,n,!0)}function oS(n,t){for(let e=nS(t);e>=0;e=mv(t,e))Ic(n,tS(t),!0)}function pv(n,t,e,r){let i=H(),o=Oe(),s=vf(2);if(o.firstUpdatePass&&bv(o,n,s,r),t!==Pt&&Vn(i,s,t)){let a=o.data[Fn()];yv(o,a,i,i[Se],n,i[s+1]=hS(t,e),r,s)}}function sS(n,t,e,r){let i=Oe(),o=vf(2);i.firstUpdatePass&&bv(i,null,o,r);let s=H();if(e!==Pt&&Vn(s,o,e)){let a=i.data[Fn()];if(_v(a,r)&&!gv(i,o)){let c=r?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=Dc(c,e||"")),_m(i,a,s,e,r)}else mS(i,a,s,s[Se],s[o+1],s[o+1]=uS(n,t,e),r,o)}}function gv(n,t){return t>=n.expandoStartIndex}function bv(n,t,e,r){let i=n.data;if(i[e+1]===null){let o=i[Fn()],s=gv(n,e);_v(o,r)&&t===null&&!s&&(t=!1),t=aS(i,o,t,r),XI(i,o,t,e,s,r)}}function aS(n,t,e,r){let i=Ab(n),o=r?t.residualClasses:t.residualStyles;if(i===null)(r?t.classBindings:t.styleBindings)===0&&(e=Ff(null,n,t,e,r),e=Ts(e,t.attrs,r),o=null);else{let s=t.directiveStylingLast;if(s===-1||n[s]!==i)if(e=Ff(i,n,t,e,r),o===null){let c=cS(n,t,r);c!==void 0&&Array.isArray(c)&&(c=Ff(null,n,t,c[1],r),c=Ts(c,t.attrs,r),lS(n,t,r,c))}else o=dS(n,t,r)}return o!==void 0&&(r?t.residualClasses=o:t.residualStyles=o),e}function cS(n,t,e){let r=e?t.classBindings:t.styleBindings;if(Eo(r)!==0)return n[xi(r)]}function lS(n,t,e,r){let i=e?t.classBindings:t.styleBindings;n[xi(i)]=r}function dS(n,t,e){let r,i=t.directiveEnd;for(let o=1+t.directiveStylingLast;o<i;o++){let s=n[o].hostAttrs;r=Ts(r,s,e)}return Ts(r,t.attrs,e)}function Ff(n,t,e,r,i){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=t[a],r=Ts(r,o.hostAttrs,i),o!==n);)a++;return n!==null&&(e.directiveStylingLast=a),r}function Ts(n,t,e){let r=e?1:2,i=-1;if(t!==null)for(let o=0;o<t.length;o++){let s=t[o];typeof s=="number"?i=s:i===r&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),Ic(n,s,e?!0:t[++o]))}return n===void 0?null:n}function uS(n,t,e){if(e==null||e==="")return gt;let r=[],i=bn(e);if(Array.isArray(i))for(let o=0;o<i.length;o++)n(r,i[o],!0);else if(i instanceof Set)for(let o of i)n(r,o,!0);else if(typeof i=="object")for(let o in i)i.hasOwnProperty(o)&&n(r,o,i[o]);else typeof i=="string"&&t(r,i);return r}function fS(n,t,e){let r=String(t);r!==""&&!r.includes(" ")&&Ic(n,r,e)}function mS(n,t,e,r,i,o,s,a){i===Pt&&(i=gt);let c=0,l=0,d=0<i.length?i[0]:null,u=0<o.length?o[0]:null;for(;d!==null||u!==null;){let m=c<i.length?i[c+1]:void 0,h=l<o.length?o[l+1]:void 0,p=null,g;d===u?(c+=2,l+=2,m!==h&&(p=u,g=h)):u===null||d!==null&&d<u?(c+=2,p=d):(l+=2,p=u,g=h),p!==null&&yv(n,t,e,r,p,g,s,a),d=c<i.length?i[c]:null,u=l<o.length?o[l]:null}}function yv(n,t,e,r,i,o,s,a){if(!(t.type&3))return;let c=n.data,l=c[a+1],d=QI(l)?wy(c,t,e,i,Eo(l),s):void 0;if(!sl(d)){sl(o)||ZI(l)&&(o=wy(c,null,e,i,a,s));let u=nf(Fn(),e);sk(r,s,u,i,o)}}function wy(n,t,e,r,i,o){let s=t===null,a;for(;i>0;){let c=n[i],l=Array.isArray(c),d=l?c[1]:c,u=d===null,m=e[i+1];m===Pt&&(m=u?gt:void 0);let h=u?Sc(m,r):d===r?m:void 0;if(l&&!sl(h)&&(h=Sc(c,r)),sl(h)&&(a=h,s))return a;let p=n[i+1];i=s?xi(p):Eo(p)}if(t!==null){let c=o?t.residualClasses:t.residualStyles;c!=null&&(a=Sc(c,r))}return a}function sl(n){return n!==void 0}function hS(n,t){return n==null||n===""||(typeof t=="string"?n=n+t:typeof n=="object"&&(n=xc(bn(n)))),n}function _v(n,t){return(n.flags&(t?8:16))!==0}function _t(n,t=""){let e=H(),r=Oe(),i=n+Ve,o=r.firstCreatePass?ki(r,i,1,t,null):r.data[i],s=pS(r,e,o,t);e[i]=s,Vc()&&Um(r,e,s,o),gi(o,!1)}var pS=(n,t,e,r)=>(bs(!0),DE(t[Se],r));function gS(n,t,e,r=""){return Vn(n,Mr(),e)?t+ds(e)+r:Pt}function Ti(n){return en("",n),Ti}function en(n,t,e){let r=H(),i=gS(r,n,t,e);return i!==Pt&&bS(r,Fn(),i),en}function bS(n,t,e){let r=nf(t,n);wE(n[Se],r,e)}function wl(n,t,e){rh(t)&&(t=t());let r=H(),i=Mr();if(Vn(r,i,t)){let o=Oe(),s=gs();C_(s,r,n,t,r[Se],e)}return wl}function uh(n,t){let e=rh(n);return e&&n.set(t),e}function Cl(n,t){let e=H(),r=Oe(),i=at();return fv(r,e,e[Se],i,n,t),Cl}var vv={};function El(n){sr("NgLet");let t=Oe(),e=H(),r=n+Ve,i=ki(t,r,128,null,null);return gi(i,!1),Ac(t,e,r,vv),El}function fh(n){let t=Oe(),e=H(),r=Fn();return Ac(t,e,r,n),n}function mh(n){let t=bf(),e=rf(t,Ve+n);if(e===vv)throw new M(314,!1);return e}function Cy(n,t,e){let r=Oe();r.firstCreatePass&&xv(t,r.data,r.blueprint,Pn(n),e)}function xv(n,t,e,r,i){if(n=ft(n),Array.isArray(n))for(let o=0;o<n.length;o++)xv(n[o],t,e,r,i);else{let o=Oe(),s=H(),a=at(),c=oi(n)?n:ft(n.provide),l=Ku(n),d=a.providerIndexes&1048575,u=a.directiveStart,m=a.providerIndexes>>20;if(oi(n)||!n.multi){let h=new _i(l,i,Ne,null),p=Bf(c,t,i?d:d+m,u);p===-1?(jf(Xc(a,s),o,c),Lf(o,n,t.length),t.push(c),a.directiveStart++,a.directiveEnd++,i&&(a.providerIndexes+=1048576),e.push(h),s.push(h)):(e[p]=h,s[p]=h)}else{let h=Bf(c,t,d+m,u),p=Bf(c,t,d,d+m),g=h>=0&&e[h],y=p>=0&&e[p];if(i&&!y||!i&&!g){jf(Xc(a,s),o,c);let D=vS(i?_S:yS,e.length,i,r,l,n);!i&&y&&(e[p].providerFactory=D),Lf(o,n,t.length,0),t.push(c),a.directiveStart++,a.directiveEnd++,i&&(a.providerIndexes+=1048576),e.push(D),s.push(D)}else{let D=Dv(e[i?p:h],l,!i&&r);Lf(o,n,h>-1?h:p,D)}!i&&r&&y&&e[p].componentProviders++}}}function Lf(n,t,e,r){let i=oi(t),o=pb(t);if(i||o){let c=(o?ft(t.useClass):t).prototype.ngOnDestroy;if(c){let l=n.destroyHooks||(n.destroyHooks=[]);if(!i&&t.multi){let d=l.indexOf(e);d===-1?l.push(e,[r,c]):l[d+1].push(r,c)}else l.push(e,c)}}}function Dv(n,t,e){return e&&n.componentProviders++,n.multi.push(t)-1}function Bf(n,t,e,r){for(let i=e;i<r;i++)if(t[i]===n)return i;return-1}function yS(n,t,e,r,i){return Dm(this.multi,[])}function _S(n,t,e,r,i){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=ws(r,r[L],this.providerFactory.index,i);s=c.slice(0,a),Dm(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],Dm(o,s);return s}function Dm(n,t){for(let e=0;e<n.length;e++){let r=n[e];t.push(r())}return t}function vS(n,t,e,r,i,o){let s=new _i(n,e,Ne,null);return s.multi=[],s.index=t,s.componentProviders=0,Dv(s,i,r&&!e),s}function Ke(n,t){return e=>{e.providersResolver=(r,i)=>Cy(r,i?i(n):n,!1),t&&(e.viewProvidersResolver=(r,i)=>Cy(r,i?i(t):t,!0))}}function hh(n,t){let e=_f()+n,r=H();return r[e]===Pt?$_(r,e,t()):eI(r,e)}function kl(n,t,e){return DS(H(),_f(),n,t,e)}function xS(n,t){let e=n[t];return e===Pt?void 0:e}function DS(n,t,e,r,i,o){let s=t+e;return Vn(n,s,i)?$_(n,s+1,o?r.call(o,i):r(i)):xS(n,s+1)}function Pr(n,t){return yl(n,t)}var wv=(()=>{class n{applicationErrorHandler=f(tr);appRef=f(Qt);taskService=f(bi);ngZone=f(F);zonelessEnabled=f(_s);tracing=f(yn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ye;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(cs):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(f(If,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let r=this.useMicrotaskScheduler?Hb:wf;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(cs+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(r){this.applicationErrorHandler(r)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Cv(){return[{provide:On,useExisting:wv},{provide:F,useClass:ls},{provide:_s,useValue:!0}]}function wS(){return typeof $localize<"u"&&$localize.locale||Ls}var Il=new x("",{factory:()=>f(Il,{optional:!0,skipSelf:!0})||wS()});function $e(n){return Qg(n)}function Me(n,t){return Ko(n,t?.equal)}var CS=n=>n;function St(n,t){if(typeof n=="function"){let e=wu(n,CS,t?.equal);return Ev(e,t?.debugName)}else{let e=wu(n.source,n.computation,n.equal);return Ev(e,n.debugName)}}function Ev(n,t){let e=n[We],r=n;return r.set=i=>Zg(e,i),r.update=i=>Kg(e,i),r.asReadonly=ys.bind(n),r}function yh(n){let t=n.request,e=n.params??t??(()=>null);return new Mo(e,kS(n),n.defaultValue,n.equal?ES(n.equal):void 0,n.debugName,n.injector??f(V))}var ph=class{value;isLoading;constructor(t,e){this.value=t,this.value.set=this.set.bind(this),this.value.update=this.update.bind(this),this.value.asReadonly=ys,this.isLoading=Me(()=>this.status()==="loading"||this.status()==="reloading",void 0)}isError=Me(()=>this.status()==="error");update(t){this.set(t($e(this.value)))}isValueDefined=Me(()=>this.isError()?!1:this.value()!==void 0);_snapshot;get snapshot(){return this._snapshot??=Me(()=>{let t=this.status();return t==="error"?{status:"error",error:this.error()}:{status:t,value:this.value()}})}hasValue(){return this.isValueDefined()}asReadonly(){return this}},Mo=class extends ph{loaderFn;equal;debugName;pendingTasks;state;extRequest;effectRef;pendingController;resolvePendingTask=void 0;destroyed=!1;unregisterOnDestroy;status;error;constructor(t,e,r,i,o,s,a){super(Me(()=>{let c=this.state().stream?.();if(!c||this.state().status==="loading"&&this.error())return r;if(!gh(c))throw new Sl(this.error());return c.value},{equal:i}),o),this.loaderFn=e,this.equal=i,this.debugName=o,this.extRequest=St({source:t,computation:c=>({request:c,reload:0})}),this.state=St({source:this.extRequest,computation:(c,l)=>{if(l){let d=c.request===void 0?"idle":"loading";return{extRequest:c,status:d,previousStatus:kv(l.value),stream:l.value.extRequest.request===c.request?l.value.stream:void 0}}else{let d=a?.(c.request);a=void 0;let u=c.request===void 0?"idle":d?"resolved":"loading";return{extRequest:c,status:u,previousStatus:"idle",stream:d}}}}),this.effectRef=bt(this.loadEffect.bind(this),{injector:s,manualCleanup:!0}),this.pendingTasks=s.get(yo),this.unregisterOnDestroy=s.get(Ot).onDestroy(()=>this.destroy()),this.status=Me(()=>kv(this.state()),void 0),this.error=Me(()=>{let c=this.state().stream?.();return c&&!gh(c)?c.error:void 0},void 0)}set(t){if(this.destroyed)return;let e=$e(this.error),r=$e(this.state);if(!e){let i=$e(this.value);if(r.status==="local"&&(this.equal?this.equal(i,t):i===t))return}this.state.set({extRequest:r.extRequest,status:"local",previousStatus:"local",stream:X({value:t},void 0)}),this.abortInProgressLoad()}reload(){let{status:t}=$e(this.state);return t==="idle"||t==="loading"?!1:(this.extRequest.update(({request:e,reload:r})=>({request:e,reload:r+1})),!0)}destroy(){this.destroyed=!0,this.unregisterOnDestroy(),this.effectRef.destroy(),this.abortInProgressLoad(),this.state.set({extRequest:{request:void 0,reload:0},status:"idle",previousStatus:"idle",stream:void 0})}async loadEffect(){let t=this.extRequest(),{status:e,previousStatus:r}=$e(this.state);if(t.request===void 0)return;if(e!=="loading")return;this.abortInProgressLoad();let i=this.resolvePendingTask=this.pendingTasks.add(),{signal:o}=this.pendingController=new AbortController;try{let s=await $e(()=>this.loaderFn({params:t.request,abortSignal:o,previous:{status:r}}));if(o.aborted||$e(this.extRequest)!==t)return;this.state.set({extRequest:t,status:"resolved",previousStatus:"resolved",stream:s})}catch(s){if(o.aborted||$e(this.extRequest)!==t)return;this.state.set({extRequest:t,status:"resolved",previousStatus:"error",stream:X({error:Bs(s)},void 0)})}finally{i?.(),i=void 0}}abortInProgressLoad(){$e(()=>this.pendingController?.abort()),this.pendingController=void 0,this.resolvePendingTask?.(),this.resolvePendingTask=void 0}};function ES(n){return(t,e)=>t===void 0||e===void 0?t===e:n(t,e)}function kS(n){return IS(n)?n.stream:async t=>{try{return X({value:await n.loader(t)},void 0)}catch(e){return X({error:Bs(e)},void 0)}}}function IS(n){return!!n.stream}function kv(n){switch(n.status){case"loading":return n.extRequest.reload===0?"loading":"reloading";case"resolved":return gh(n.stream())?"resolved":"error";default:return n.status}}function gh(n){return n.error===void 0}function Bs(n){return SS(n)?n:new bh(n)}function SS(n){return n instanceof Error||typeof n=="object"&&typeof n.name=="string"&&typeof n.message=="string"}var Sl=class extends Error{constructor(t){super(t.message,{cause:t})}},bh=class extends Error{constructor(t){super(String(t),{cause:t})}};var Rv=Symbol("InputSignalNode#UNSET"),WS=W(w({},Qo),{transformFn:void 0,applyValueToInputSignal(n,t){qr(n,t)}});function Ov(n,t){let e=Object.create(WS);e.value=n,e.transformFn=t?.transform;function r(){if(br(e),e.value===Rv){let i=null;throw new M(-950,i)}return e.value}return r[We]=e,r}var Fr=class{attributeName;constructor(t){this.attributeName=t}__NG_ELEMENT_ID__=()=>Cm(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function Iv(n,t){return Ov(n,t)}function qS(n){return Ov(Rv,n)}var Nv=(Iv.required=qS,Iv);function Sv(n,t){return th(t)}function YS(n,t){return nh(t)}var Mi=(Sv.required=YS,Sv);function Tv(n,t){return th(t)}function ZS(n,t){return nh(t)}var Pv=(Tv.required=ZS,Tv);var vh=new x(""),KS=new x("");function Vs(n){return!n.moduleRef}function QS(n){let t=Vs(n)?n.r3Injector:n.moduleRef.injector,e=t.get(F);return e.run(()=>{Vs(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let r=t.get(tr),i;if(e.runOutsideAngular(()=>{i=e.onError.subscribe({next:r})}),Vs(n)){let o=()=>t.destroy(),s=n.platformInjector.get(vh);s.add(o),t.onDestroy(()=>{i.unsubscribe(),s.delete(o)})}else{let o=()=>n.moduleRef.destroy(),s=n.platformInjector.get(vh);s.add(o),n.moduleRef.onDestroy(()=>{Ds(n.allPlatformModules,n.moduleRef),i.unsubscribe(),s.delete(o)})}return JS(r,e,()=>{let o=t.get(bi),s=o.add(),a=t.get(sh);return a.runInitializers(),a.donePromise.then(()=>{let c=t.get(Il,Ls);if(uv(c||Ls),!t.get(KS,!0))return Vs(n)?t.get(Qt):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Vs(n)){let d=t.get(Qt);return n.rootComponent!==void 0&&d.bootstrap(n.rootComponent),d}else return XS?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{o.remove(s)})})})}var XS;function JS(n,t,e){try{let r=e();return Io(r)?r.catch(i=>{throw t.runOutsideAngular(()=>n(i)),i}):r}catch(r){throw t.runOutsideAngular(()=>n(r)),r}}var Tl=null;function eT(n=[],t){return V.create({name:t,providers:[{provide:fs,useValue:"platform"},{provide:vh,useValue:new Set([()=>Tl=null])},...n]})}function tT(n=[]){if(Tl)return Tl;let t=eT(n);return Tl=t,cv(),nT(t),t}function nT(n){let t=n.get(al,null);uo(n,()=>{t?.forEach(e=>e())})}var rT=1e4;var sz=rT-1e3;var tt=(()=>{class n{static __NG_ELEMENT_ID__=iT}return n})();function iT(n){return oT(at(),H(),(n&16)===16)}function oT(n,t,e){if(er(n)&&!e){let r=Wt(n.index,t);return new Rr(r,r)}else if(n.type&175){let r=t[kt];return new Rr(r,t)}return null}function Fv(n){let{rootComponent:t,appProviders:e,platformProviders:r,platformRef:i}=n;Ee(ve.BootstrapApplicationStart);try{let o=i?.injector??tT(r),s=[Cv(),Ub,...e||[]],a=new Is({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return QS({r3Injector:a.injector,platformInjector:o,rootComponent:t})}catch(o){return Promise.reject(o)}finally{Ee(ve.BootstrapApplicationEnd)}}function re(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}function jt(n,t=NaN){return!isNaN(parseFloat(n))&&!isNaN(Number(n))?Number(n):t}var _h=Symbol("NOT_SET"),Lv=new Set,sT=W(w({},Qo),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:_h,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(n){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==_h&&!Qi(this))return this.signal;try{for(let i of this.cleanup??Lv)i()}finally{this.cleanup?.clear()}let t=[];n!==void 0&&t.push(n),t.push(this.registerCleanupFn);let e=Yn(this),r;try{r=this.userFn.apply(null,t)}finally{yr(this,e)}return(this.value===_h||!this.equal(this.value,r))&&(this.value=r,this.version++),this.signal}}),xh=class extends Cs{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(t,e,r,i,o,s=null){super(t,[void 0,void 0,void 0,void 0],r,!1,o.get(Ot),s),this.scheduler=i;for(let a of jm){let c=e[a];if(c===void 0)continue;let l=Object.create(sT);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(br(l),l.value),l.signal[We]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();super.destroy();for(let t of this.nodes)if(t)try{for(let e of t.cleanup??Lv)e()}finally{_r(t)}}};function Bv(n,t){let e=t?.injector??f(V),r=e.get(On),i=e.get(hl),o=e.get(yn,null,{optional:!0});i.impl??=e.get(Hm);let s=n;typeof s=="function"&&(s={mixedReadWrite:n});let a=e.get(bo,null,{optional:!0}),c=new xh(i.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,r,e,o?.snapshot(null));return i.impl.register(c),c}function Ml(n,t){let e=Er(n),r=t.elementInjector||lo();return new Co(e).create(r,t.projectableNodes,t.hostElement,t.environmentInjector,t.directives,t.bindings)}var Vv=null;function tn(){return Vv}function Dh(n){Vv??=n}var js=class{},Al=(()=>{class n{historyGo(e){throw new Error("")}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:()=>f(jv),providedIn:"platform"})}return n})();var jv=(()=>{class n extends Al{_location;_history;_doc=f(B);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return tn().getBaseHref(this._doc)}onPopState(e){let r=tn().getGlobalEventTarget(this._doc,"window");return r.addEventListener("popstate",e,!1),()=>r.removeEventListener("popstate",e)}onHashChange(e){let r=tn().getGlobalEventTarget(this._doc,"window");return r.addEventListener("hashchange",e,!1),()=>r.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,r,i){this._history.pushState(e,r,i)}replaceState(e,r,i){this._history.replaceState(e,r,i)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:()=>new n,providedIn:"platform"})}return n})();function Uv(n,t){return n?t?n.endsWith("/")?t.startsWith("/")?n+t.slice(1):n+t:t.startsWith("/")?n+t:`${n}/${t}`:n:t}function Hv(n){let t=n.search(/#|\?|$/);return n[t-1]==="/"?n.slice(0,t-1)+n.slice(t):n}function Lr(n){return n&&n[0]!=="?"?`?${n}`:n}var Rl=(()=>{class n{historyGo(e){throw new Error("")}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:()=>f(cT),providedIn:"root"})}return n})(),aT=new x(""),cT=(()=>{class n extends Rl{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,r){super(),this._platformLocation=e,this._baseHref=r??this._platformLocation.getBaseHrefFromDOM()??f(B).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return Uv(this._baseHref,e)}path(e=!1){let r=this._platformLocation.pathname+Lr(this._platformLocation.search),i=this._platformLocation.hash;return i&&e?`${r}${i}`:r}pushState(e,r,i,o){let s=this.prepareExternalUrl(i+Lr(o));this._platformLocation.pushState(e,r,s)}replaceState(e,r,i,o){let s=this.prepareExternalUrl(i+Lr(o));this._platformLocation.replaceState(e,r,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(r){return new(r||n)(K(Al),K(aT,8))};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Ol=(()=>{class n{_subject=new E;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let r=this._locationStrategy.getBaseHref();this._basePath=uT(Hv(zv(r))),this._locationStrategy.onPopState(i=>{this._subject.next({url:this.path(!0),pop:!0,state:i.state,type:i.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,r=""){return this.path()==this.normalize(e+Lr(r))}normalize(e){return n.stripTrailingSlash(dT(this._basePath,zv(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,r="",i=null){this._locationStrategy.pushState(i,"",e,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Lr(r)),i)}replaceState(e,r="",i=null){this._locationStrategy.replaceState(i,"",e,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Lr(r)),i)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(r=>{this._notifyUrlChangeListeners(r.url,r.state)}),()=>{let r=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(r,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",r){this._urlChangeListeners.forEach(i=>i(e,r))}subscribe(e,r,i){return this._subject.subscribe({next:e,error:r??void 0,complete:i??void 0})}static normalizeQueryParams=Lr;static joinWithSlash=Uv;static stripTrailingSlash=Hv;static \u0275fac=function(r){return new(r||n)(K(Rl))};static \u0275prov=C({token:n,factory:()=>lT(),providedIn:"root"})}return n})();function lT(){return new Ol(K(Rl))}function dT(n,t){if(!n||!t.startsWith(n))return t;let e=t.substring(n.length);return e===""||["/",";","?","#"].includes(e[0])?e:t}function zv(n){return n.replace(/\/index.html$/,"")}function uT(n){if(new RegExp("^(https?:)?//").test(n)){let[,e]=n.split(/\/\/[^\/]+/);return e}return n}var Ai=(()=>{class n{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=f(V);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let r=this._viewContainerRef;if(this._viewRef&&r.remove(r.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let i=this._createContextForwardProxy();this._viewRef=r.createEmbeddedView(this.ngTemplateOutlet,i,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,r,i)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,r,i):!1,get:(e,r,i)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,r,i)}})}static \u0275fac=function(r){return new(r||n)(Ne(Lt))};static \u0275dir=Y({type:n,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[it]})}return n})();function Nl(n,t){t=encodeURIComponent(t);for(let e of n.split(";")){let r=e.indexOf("="),[i,o]=r==-1?[e,""]:[e.slice(0,r),e.slice(r+1)];if(i.trim()===t)return decodeURIComponent(o)}return null}var Ri=class{};var wh="browser";function $v(n){return n===wh}var Hs=class{_doc;constructor(t){this._doc=t}manager},Pl=(()=>{class n extends Hs{constructor(e){super(e)}supports(e){return!0}addEventListener(e,r,i,o){return e.addEventListener(r,i,o),()=>this.removeEventListener(e,r,i,o)}removeEventListener(e,r,i,o){return e.removeEventListener(r,i,o)}static \u0275fac=function(r){return new(r||n)(K(B))};static \u0275prov=C({token:n,factory:n.\u0275fac})}return n})(),Bl=new x(""),Ih=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,r){this._zone=r,e.forEach(s=>{s.manager=this});let i=e.filter(s=>!(s instanceof Pl));this._plugins=i.slice().reverse();let o=e.find(s=>s instanceof Pl);o&&this._plugins.push(o)}addEventListener(e,r,i,o){return this._findPluginFor(r).addEventListener(e,r,i,o)}getZone(){return this._zone}_findPluginFor(e){let r=this._eventNameToPlugin.get(e);if(r)return r;if(r=this._plugins.find(o=>o.supports(e)),!r)throw new M(5101,!1);return this._eventNameToPlugin.set(e,r),r}static \u0275fac=function(r){return new(r||n)(K(Bl),K(F))};static \u0275prov=C({token:n,factory:n.\u0275fac})}return n})(),Ch="ng-app-id";function Gv(n){for(let t of n)t.remove()}function Wv(n,t){let e=t.createElement("style");return e.textContent=n,e}function pT(n,t,e,r){let i=n.head?.querySelectorAll(`style[${Ch}="${t}"],link[${Ch}="${t}"]`);if(i)for(let o of i)o.removeAttribute(Ch),o instanceof HTMLLinkElement?r.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function kh(n,t){let e=t.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",n),e}var Sh=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,r,i,o={}){this.doc=e,this.appId=r,this.nonce=i,pT(e,r,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,r){for(let i of e)this.addUsage(i,this.inline,Wv);r?.forEach(i=>this.addUsage(i,this.external,kh))}removeStyles(e,r){for(let i of e)this.removeUsage(i,this.inline);r?.forEach(i=>this.removeUsage(i,this.external))}addUsage(e,r,i){let o=r.get(e);o?o.usage++:r.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,i(e,this.doc)))})}removeUsage(e,r){let i=r.get(e);i&&(i.usage--,i.usage<=0&&(Gv(i.elements),r.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])Gv(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[r,{elements:i}]of this.inline)i.push(this.addElement(e,Wv(r,this.doc)));for(let[r,{elements:i}]of this.external)i.push(this.addElement(e,kh(r,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,r){return this.nonce&&r.setAttribute("nonce",this.nonce),e.appendChild(r)}static \u0275fac=function(r){return new(r||n)(K(B),K(Di),K(Ci,8),K(wi))};static \u0275prov=C({token:n,factory:n.\u0275fac})}return n})(),Eh={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Th=/%COMP%/g;var Yv="%COMP%",gT=`_nghost-${Yv}`,bT=`_ngcontent-${Yv}`,yT=!0,_T=new x("",{factory:()=>yT});function vT(n){return bT.replace(Th,n)}function xT(n){return gT.replace(Th,n)}function Zv(n,t){return t.map(e=>e.replace(Th,n))}var Mh=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,r,i,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=r,this.appId=i,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new zs(e,s,a,this.tracingService)}createRenderer(e,r){if(!e||!r)return this.defaultRenderer;let i=this.getOrCreateRenderer(e,r);return i instanceof Ll?i.applyToHost(e):i instanceof Us&&i.applyStyles(),i}getOrCreateRenderer(e,r){let i=this.rendererByCompId,o=i.get(r.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(r.encapsulation){case gn.Emulated:o=new Ll(c,l,r,this.appId,d,s,a,u);break;case gn.ShadowDom:return new Fl(c,e,r,s,a,this.nonce,u,l);case gn.ExperimentalIsolatedShadowDom:return new Fl(c,e,r,s,a,this.nonce,u);default:o=new Us(c,l,r,d,s,a,u);break}i.set(r.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(r){return new(r||n)(K(Ih),K(Sh),K(Di),K(_T),K(B),K(F),K(Ci),K(yn,8))};static \u0275prov=C({token:n,factory:n.\u0275fac})}return n})(),zs=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(t,e,r,i){this.eventManager=t,this.doc=e,this.ngZone=r,this.tracingService=i}destroy(){}destroyNode=null;createElement(t,e){return e?this.doc.createElementNS(Eh[e]||e,t):this.doc.createElement(t)}createComment(t){return this.doc.createComment(t)}createText(t){return this.doc.createTextNode(t)}appendChild(t,e){(qv(t)?t.content:t).appendChild(e)}insertBefore(t,e,r){t&&(qv(t)?t.content:t).insertBefore(e,r)}removeChild(t,e){e.remove()}selectRootElement(t,e){let r=typeof t=="string"?this.doc.querySelector(t):t;if(!r)throw new M(-5104,!1);return e||(r.textContent=""),r}parentNode(t){return t.parentNode}nextSibling(t){return t.nextSibling}setAttribute(t,e,r,i){if(i){e=i+":"+e;let o=Eh[i];o?t.setAttributeNS(o,e,r):t.setAttribute(e,r)}else t.setAttribute(e,r)}removeAttribute(t,e,r){if(r){let i=Eh[r];i?t.removeAttributeNS(i,e):t.removeAttribute(`${r}:${e}`)}else t.removeAttribute(e)}addClass(t,e){t.classList.add(e)}removeClass(t,e){t.classList.remove(e)}setStyle(t,e,r,i){i&(Bn.DashCase|Bn.Important)?t.style.setProperty(e,r,i&Bn.Important?"important":""):t.style[e]=r}removeStyle(t,e,r){r&Bn.DashCase?t.style.removeProperty(e):t.style[e]=""}setProperty(t,e,r){t!=null&&(t[e]=r)}setValue(t,e){t.nodeValue=e}listen(t,e,r,i){if(typeof t=="string"&&(t=tn().getGlobalEventTarget(this.doc,t),!t))throw new M(5102,!1);let o=this.decoratePreventDefault(r);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(t,e,o)),this.eventManager.addEventListener(t,e,o,i)}decoratePreventDefault(t){return e=>{if(e==="__ngUnwrap__")return t;t(e)===!1&&e.preventDefault()}}};function qv(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Fl=class extends zs{hostEl;sharedStylesHost;shadowRoot;constructor(t,e,r,i,o,s,a,c){super(t,i,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=r.styles;l=Zv(r.id,l);for(let u of l){let m=document.createElement("style");s&&m.setAttribute("nonce",s),m.textContent=u,this.shadowRoot.appendChild(m)}let d=r.getExternalStyles?.();if(d)for(let u of d){let m=kh(u,i);s&&m.setAttribute("nonce",s),this.shadowRoot.appendChild(m)}}nodeOrShadowRoot(t){return t===this.hostEl?this.shadowRoot:t}appendChild(t,e){return super.appendChild(this.nodeOrShadowRoot(t),e)}insertBefore(t,e,r){return super.insertBefore(this.nodeOrShadowRoot(t),e,r)}removeChild(t,e){return super.removeChild(null,e)}parentNode(t){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(t)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Us=class extends zs{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(t,e,r,i,o,s,a,c){super(t,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=i;let l=r.styles;this.styles=c?Zv(c,l):l,this.styleUrls=r.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&vi.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Ll=class extends Us{contentAttr;hostAttr;constructor(t,e,r,i,o,s,a,c){let l=i+"-"+r.id;super(t,e,r,o,s,a,c,l),this.contentAttr=vT(l),this.hostAttr=xT(l)}applyToHost(t){this.applyStyles(),this.setAttribute(t,this.hostAttr,"")}createElement(t,e){let r=super.createElement(t,e);return super.setAttribute(r,this.contentAttr,""),r}};var Vl=class n extends js{supportsDOMEvents=!0;static makeCurrent(){Dh(new n)}onAndCancel(t,e,r,i){return t.addEventListener(e,r,i),()=>{t.removeEventListener(e,r,i)}}dispatchEvent(t,e){t.dispatchEvent(e)}remove(t){t.remove()}createElement(t,e){return e=e||this.getDefaultDocument(),e.createElement(t)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(t){return t.nodeType===Node.ELEMENT_NODE}isShadowRoot(t){return t instanceof DocumentFragment}getGlobalEventTarget(t,e){return e==="window"?window:e==="document"?t:e==="body"?t.body:null}getBaseHref(t){let e=DT();return e==null?null:wT(e)}resetBaseElement(){$s=null}getUserAgent(){return window.navigator.userAgent}getCookie(t){return Nl(document.cookie,t)}},$s=null;function DT(){return $s=$s||document.head.querySelector("base"),$s?$s.getAttribute("href"):null}function wT(n){return new URL(n,document.baseURI).pathname}var CT=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac})}return n})(),Kv=["alt","control","meta","shift"],ET={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},kT={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},Qv=(()=>{class n extends Hs{constructor(e){super(e)}supports(e){return n.parseEventName(e)!=null}addEventListener(e,r,i,o){let s=n.parseEventName(r),a=n.eventCallback(s.fullKey,i,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>tn().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let r=e.toLowerCase().split("."),i=r.shift();if(r.length===0||!(i==="keydown"||i==="keyup"))return null;let o=n._normalizeKey(r.pop()),s="",a=r.indexOf("code");if(a>-1&&(r.splice(a,1),s="code."),Kv.forEach(l=>{let d=r.indexOf(l);d>-1&&(r.splice(d,1),s+=l+".")}),s+=o,r.length!=0||o.length===0)return null;let c={};return c.domEventName=i,c.fullKey=s,c}static matchEventFullKeyCode(e,r){let i=ET[e.key]||e.key,o="";return r.indexOf("code.")>-1&&(i=e.code,o="code."),i==null||!i?!1:(i=i.toLowerCase(),i===" "?i="space":i==="."&&(i="dot"),Kv.forEach(s=>{if(s!==i){let a=kT[s];a(e)&&(o+=s+".")}}),o+=i,o===r)}static eventCallback(e,r,i){return o=>{n.matchEventFullKeyCode(o,e)&&i.runGuarded(()=>r(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(r){return new(r||n)(K(B))};static \u0275prov=C({token:n,factory:n.\u0275fac})}return n})();async function Ah(n,t,e){let r=w({rootComponent:n},IT(t,e));return Fv(r)}function IT(n,t){return{platformRef:t?.platformRef,appProviders:[...RT,...n?.providers??[]],platformProviders:AT}}function ST(){Vl.makeCurrent()}function TT(){return new cn}function MT(){return km(document),document}var AT=[{provide:wi,useValue:wh},{provide:al,useValue:ST,multi:!0},{provide:B,useFactory:MT}];var RT=[{provide:fs,useValue:"root"},{provide:cn,useFactory:TT},{provide:Bl,useClass:Pl,multi:!0},{provide:Bl,useClass:Qv,multi:!0},Mh,Sh,Ih,{provide:ct,useExisting:Mh},{provide:Ri,useClass:CT},[]];var Dn=class n{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(t){t?typeof t=="string"?this.lazyInit=()=>{this.headers=new Map,t.split(`
`).forEach(e=>{let r=e.indexOf(":");if(r>0){let i=e.slice(0,r),o=e.slice(r+1).trim();this.addHeaderEntry(i,o)}})}:typeof Headers<"u"&&t instanceof Headers?(this.headers=new Map,t.forEach((e,r)=>{this.addHeaderEntry(r,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(t).forEach(([e,r])=>{this.setHeaderEntries(e,r)})}:this.headers=new Map}has(t){return this.init(),this.headers.has(t.toLowerCase())}get(t){this.init();let e=this.headers.get(t.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(t){return this.init(),this.headers.get(t.toLowerCase())||null}append(t,e){return this.clone({name:t,value:e,op:"a"})}set(t,e){return this.clone({name:t,value:e,op:"s"})}delete(t,e){return this.clone({name:t,value:e,op:"d"})}maybeSetNormalizedName(t,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,t)}init(){this.lazyInit&&(this.lazyInit instanceof n?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(t=>this.applyUpdate(t)),this.lazyUpdate=null))}copyFrom(t){t.init(),Array.from(t.headers.keys()).forEach(e=>{this.headers.set(e,t.headers.get(e)),this.normalizedNames.set(e,t.normalizedNames.get(e))})}clone(t){let e=new n;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof n?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([t]),e}applyUpdate(t){let e=t.name.toLowerCase();switch(t.op){case"a":case"s":let r=t.value;if(typeof r=="string"&&(r=[r]),r.length===0)return;this.maybeSetNormalizedName(t.name,e);let i=(t.op==="a"?this.headers.get(e):void 0)||[];i.push(...r),this.headers.set(e,i);break;case"d":let o=t.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(t,e){let r=t.toLowerCase();this.maybeSetNormalizedName(t,r),this.headers.has(r)?this.headers.get(r).push(e):this.headers.set(r,[e])}setHeaderEntries(t,e){let r=(Array.isArray(e)?e:[e]).map(o=>o.toString()),i=t.toLowerCase();this.headers.set(i,r),this.maybeSetNormalizedName(t,i)}forEach(t){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>t(this.normalizedNames.get(e),this.headers.get(e)))}};var Oh=class{map=new Map;set(t,e){return this.map.set(t,e),this}get(t){return this.map.has(t)||this.map.set(t,t.defaultValue()),this.map.get(t)}delete(t){return this.map.delete(t),this}has(t){return this.map.has(t)}keys(){return this.map.keys()}},Nh=class{encodeKey(t){return Xv(t)}encodeValue(t){return Xv(t)}decodeKey(t){return decodeURIComponent(t)}decodeValue(t){return decodeURIComponent(t)}};function OT(n,t){let e=new Map;return n.length>0&&n.replace(/^\?/,"").split("&").forEach(i=>{let o=i.indexOf("="),[s,a]=o==-1?[t.decodeKey(i),""]:[t.decodeKey(i.slice(0,o)),t.decodeValue(i.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var NT=/%(\d[a-f0-9])/gi,PT={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function Xv(n){return encodeURIComponent(n).replace(NT,(t,e)=>PT[e]??t)}function jl(n){return`${n}`}var xn=class n{map;encoder;updates=null;cloneFrom=null;constructor(t={}){if(this.encoder=t.encoder||new Nh,t.fromString){if(t.fromObject)throw new M(2805,!1);this.map=OT(t.fromString,this.encoder)}else t.fromObject?(this.map=new Map,Object.keys(t.fromObject).forEach(e=>{let r=t.fromObject[e],i=Array.isArray(r)?r.map(jl):[jl(r)];this.map.set(e,i)})):this.map=null}has(t){return this.init(),this.map.has(t)}get(t){this.init();let e=this.map.get(t);return e?e[0]:null}getAll(t){return this.init(),this.map.get(t)||null}keys(){return this.init(),Array.from(this.map.keys())}append(t,e){return this.clone({param:t,value:e,op:"a"})}appendAll(t){let e=[];return Object.keys(t).forEach(r=>{let i=t[r];Array.isArray(i)?i.forEach(o=>{e.push({param:r,value:o,op:"a"})}):e.push({param:r,value:i,op:"a"})}),this.clone(e)}set(t,e){return this.clone({param:t,value:e,op:"s"})}delete(t,e){return this.clone({param:t,value:e,op:"d"})}toString(){return this.init(),this.keys().map(t=>{let e=this.encoder.encodeKey(t);return this.map.get(t).map(r=>e+"="+this.encoder.encodeValue(r)).join("&")}).filter(t=>t!=="").join("&")}clone(t){let e=new n({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(t),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(t=>this.map.set(t,this.cloneFrom.map.get(t))),this.updates.forEach(t=>{switch(t.op){case"a":case"s":let e=(t.op==="a"?this.map.get(t.param):void 0)||[];e.push(jl(t.value)),this.map.set(t.param,e);break;case"d":if(t.value!==void 0){let r=this.map.get(t.param)||[],i=r.indexOf(jl(t.value));i!==-1&&r.splice(i,1),r.length>0?this.map.set(t.param,r):this.map.delete(t.param)}else{this.map.delete(t.param);break}}}),this.cloneFrom=this.updates=null)}};function FT(n){switch(n){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function Jv(n){return typeof ArrayBuffer<"u"&&n instanceof ArrayBuffer}function e0(n){return typeof Blob<"u"&&n instanceof Blob}function t0(n){return typeof FormData<"u"&&n instanceof FormData}function LT(n){return typeof URLSearchParams<"u"&&n instanceof URLSearchParams}var n0="Content-Type",r0="Accept",i0="text/plain",o0="application/json",BT=`${o0}, ${i0}, */*`,Oi=class n{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(t,e,r,i){this.url=e,this.method=t.toUpperCase();let o;if(FT(this.method)||i?(this.body=r!==void 0?r:null,o=i):o=r,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new M(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Dn,this.context??=new Oh,!this.params)this.params=new xn,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e.indexOf("?"),c=a===-1?"?":a<e.length-1?"&":"";this.urlWithParams=e+c+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||Jv(this.body)||e0(this.body)||t0(this.body)||LT(this.body)?this.body:this.body instanceof xn?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||t0(this.body)?null:e0(this.body)?this.body.type||null:Jv(this.body)?null:typeof this.body=="string"?i0:this.body instanceof xn?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?o0:null}clone(t={}){let e=t.method||this.method,r=t.url||this.url,i=t.responseType||this.responseType,o=t.keepalive??this.keepalive,s=t.priority||this.priority,a=t.cache||this.cache,c=t.mode||this.mode,l=t.redirect||this.redirect,d=t.credentials||this.credentials,u=t.referrer||this.referrer,m=t.integrity||this.integrity,h=t.referrerPolicy||this.referrerPolicy,p=t.transferCache??this.transferCache,g=t.timeout??this.timeout,y=t.body!==void 0?t.body:this.body,D=t.withCredentials??this.withCredentials,R=t.reportProgress??this.reportProgress,oe=t.headers||this.headers,Z=t.params||this.params,we=t.context??this.context;return t.setHeaders!==void 0&&(oe=Object.keys(t.setHeaders).reduce((me,de)=>me.set(de,t.setHeaders[de]),oe)),t.setParams&&(Z=Object.keys(t.setParams).reduce((me,de)=>me.set(de,t.setParams[de]),Z)),new n(e,r,y,{params:Z,headers:oe,context:we,reportProgress:R,responseType:i,withCredentials:D,transferCache:p,keepalive:o,cache:a,priority:s,timeout:g,mode:c,redirect:l,credentials:d,referrer:u,integrity:m,referrerPolicy:h})}},lr=(function(n){return n[n.Sent=0]="Sent",n[n.UploadProgress=1]="UploadProgress",n[n.ResponseHeader=2]="ResponseHeader",n[n.DownloadProgress=3]="DownloadProgress",n[n.Response=4]="Response",n[n.User=5]="User",n})(lr||{}),Gs=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(t,e=200,r="OK"){this.headers=t.headers||new Dn,this.status=t.status!==void 0?t.status:e,this.statusText=t.statusText||r,this.url=t.url||null,this.redirected=t.redirected,this.responseType=t.responseType,this.ok=this.status>=200&&this.status<300}},Ph=class n extends Gs{constructor(t={}){super(t)}type=lr.ResponseHeader;clone(t={}){return new n({headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0})}},Ao=class n extends Gs{body;constructor(t={}){super(t),this.body=t.body!==void 0?t.body:null}type=lr.Response;clone(t={}){return new n({body:t.body!==void 0?t.body:this.body,headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0,redirected:t.redirected??this.redirected,responseType:t.responseType??this.responseType})}},Ni=class extends Gs{name="HttpErrorResponse";message;error;ok=!1;constructor(t){super(t,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${t.url||"(unknown url)"}`:this.message=`Http failure response for ${t.url||"(unknown url)"}: ${t.status} ${t.statusText}`,this.error=t.error||null}},VT=200,jT=204;var HT=/^\)\]\}',?\n/;var zT=(()=>{class n{xhrFactory;tracingService=f(yn,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new M(-2800,!1);let r=this.xhrFactory;return An(null).pipe(ti(()=>new le(o=>{let s=r.build();if(s.open(e.method,e.urlWithParams),e.withCredentials&&(s.withCredentials=!0),e.headers.forEach((y,D)=>s.setRequestHeader(y,D.join(","))),e.headers.has(r0)||s.setRequestHeader(r0,BT),!e.headers.has(n0)){let y=e.detectContentTypeHeader();y!==null&&s.setRequestHeader(n0,y)}if(e.timeout&&(s.timeout=e.timeout),e.responseType){let y=e.responseType.toLowerCase();s.responseType=y!=="json"?y:"text"}let a=e.serializeBody(),c=null,l=()=>{if(c!==null)return c;let y=s.statusText||"OK",D=new Dn(s.getAllResponseHeaders()),R=s.responseURL||e.url;return c=new Ph({headers:D,status:s.status,statusText:y,url:R}),c},d=this.maybePropagateTrace(()=>{let{headers:y,status:D,statusText:R,url:oe}=l(),Z=null;D!==jT&&(Z=typeof s.response>"u"?s.responseText:s.response),D===0&&(D=Z?VT:0);let we=D>=200&&D<300;if(e.responseType==="json"&&typeof Z=="string"){let me=Z;Z=Z.replace(HT,"");try{Z=Z!==""?JSON.parse(Z):null}catch(de){Z=me,we&&(we=!1,Z={error:de,text:Z})}}we?(o.next(new Ao({body:Z,headers:y,status:D,statusText:R,url:oe||void 0})),o.complete()):o.error(new Ni({error:Z,headers:y,status:D,statusText:R,url:oe||void 0}))}),u=this.maybePropagateTrace(y=>{let{url:D}=l(),R=new Ni({error:y,status:s.status||0,statusText:s.statusText||"Unknown Error",url:D||void 0});o.error(R)}),m=u;e.timeout&&(m=this.maybePropagateTrace(y=>{let{url:D}=l(),R=new Ni({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:D||void 0});o.error(R)}));let h=!1,p=this.maybePropagateTrace(y=>{h||(o.next(l()),h=!0);let D={type:lr.DownloadProgress,loaded:y.loaded};y.lengthComputable&&(D.total=y.total),e.responseType==="text"&&s.responseText&&(D.partialText=s.responseText),o.next(D)}),g=this.maybePropagateTrace(y=>{let D={type:lr.UploadProgress,loaded:y.loaded};y.lengthComputable&&(D.total=y.total),o.next(D)});return s.addEventListener("load",d),s.addEventListener("error",u),s.addEventListener("timeout",m),s.addEventListener("abort",u),e.reportProgress&&(s.addEventListener("progress",p),a!==null&&s.upload&&s.upload.addEventListener("progress",g)),s.send(a),o.next({type:lr.Sent}),()=>{s.removeEventListener("error",u),s.removeEventListener("abort",u),s.removeEventListener("load",d),s.removeEventListener("timeout",m),e.reportProgress&&(s.removeEventListener("progress",p),a!==null&&s.upload&&s.upload.removeEventListener("progress",g)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(r){return new(r||n)(K(Ri))};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function UT(n,t){return t(n)}function $T(n,t,e){return(r,i)=>uo(e,()=>t(r,o=>n(o,i)))}var GT=new x("",{factory:()=>[]}),s0=new x(""),WT=new x("",{factory:()=>!0});var qT=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:function(r){let i=null;return r?i=new(r||n):i=K(zT),i},providedIn:"root"})}return n})();var YT=(()=>{class n{backend;injector;chain=null;pendingTasks=f(yo);contributeToStability=f(WT);constructor(e,r){this.backend=e,this.injector=r}handle(e){if(this.chain===null){let r=Array.from(new Set([...this.injector.get(GT),...this.injector.get(s0,[])]));this.chain=r.reduceRight((i,o)=>$T(i,o,this.injector),UT)}if(this.contributeToStability){let r=this.pendingTasks.add();return this.chain(e,i=>this.backend.handle(i)).pipe(yu(r))}else return this.chain(e,r=>this.backend.handle(r))}static \u0275fac=function(r){return new(r||n)(K(qT),K(qe))};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),ZT=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:function(r){let i=null;return r?i=new(r||n):i=K(YT),i},providedIn:"root"})}return n})();function Rh(n,t){return{body:t,headers:n.headers,context:n.context,observe:n.observe,params:n.params,reportProgress:n.reportProgress,responseType:n.responseType,withCredentials:n.withCredentials,credentials:n.credentials,transferCache:n.transferCache,timeout:n.timeout,keepalive:n.keepalive,priority:n.priority,cache:n.cache,mode:n.mode,redirect:n.redirect,integrity:n.integrity,referrer:n.referrer,referrerPolicy:n.referrerPolicy}}var a0=(()=>{class n{handler;constructor(e){this.handler=e}request(e,r,i={}){let o;if(e instanceof Oi)o=e;else{let c;i.headers instanceof Dn?c=i.headers:c=new Dn(i.headers);let l;i.params&&(i.params instanceof xn?l=i.params:l=new xn({fromObject:i.params})),o=new Oi(e,r,i.body!==void 0?i.body:null,{headers:c,context:i.context,params:l,reportProgress:i.reportProgress,responseType:i.responseType||"json",withCredentials:i.withCredentials,transferCache:i.transferCache,keepalive:i.keepalive,priority:i.priority,cache:i.cache,mode:i.mode,redirect:i.redirect,credentials:i.credentials,referrer:i.referrer,referrerPolicy:i.referrerPolicy,integrity:i.integrity,timeout:i.timeout})}let s=An(o).pipe(bu(c=>this.handler.handle(c)));if(e instanceof Oi||i.observe==="events")return s;let a=s.pipe(Ie(c=>c instanceof Ao));switch(i.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(Le(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new M(2806,!1);return c.body}));case"blob":return a.pipe(Le(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new M(2807,!1);return c.body}));case"text":return a.pipe(Le(c=>{if(c.body!==null&&typeof c.body!="string")throw new M(2808,!1);return c.body}));default:return a.pipe(Le(c=>c.body))}case"response":return a;default:throw new M(2809,!1)}}delete(e,r={}){return this.request("DELETE",e,r)}get(e,r={}){return this.request("GET",e,r)}head(e,r={}){return this.request("HEAD",e,r)}jsonp(e,r){return this.request("JSONP",e,{params:new xn().append(r,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,r={}){return this.request("OPTIONS",e,r)}patch(e,r,i={}){return this.request("PATCH",e,Rh(i,r))}post(e,r,i={}){return this.request("POST",e,Rh(i,r))}put(e,r,i={}){return this.request("PUT",e,Rh(i,r))}static \u0275fac=function(r){return new(r||n)(K(ZT))};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var KT=new x(""),QT="b",XT="h",JT="s",eM="st",tM="u",nM="rt",rM=new x(""),iM=["GET","HEAD"];function oM(n,t){let s=t,{isCacheActive:e}=s,r=fg(s,["isCacheActive"]),{transferCache:i,method:o}=n;return!(!e||i===!1||o==="POST"&&!r.includePostRequests&&!i||o!=="POST"&&!iM.includes(o)||!r.includeRequestsWithAuthHeaders&&cM(n)||r.filter?.(n)===!1)}function sM(n,t){let{includeHeaders:e}=n,r=e;return typeof t=="object"&&t.includeHeaders&&(r=t.includeHeaders),r}function aM(n,t,e,r){let{transferCache:i}=n;if(!oM(n,t))return null;if(r)throw new M(2803,!1);let o=n.url,s=lM(n,o),a=e.get(s,null),c=sM(t,i);if(a){let{[QT]:l,[nM]:d,[XT]:u,[JT]:m,[eM]:h,[tM]:p}=a,g=l;switch(d){case"arraybuffer":g=l0(l);break;case"blob":g=new Blob([l0(l)]);break}let y=new Dn(u);return new Ao({body:g,headers:y,status:m,statusText:h,url:p})}return null}function cM(n){return n.headers.has("authorization")||n.headers.has("proxy-authorization")}function c0(n){return[...n.keys()].sort().map(t=>`${t}=${n.getAll(t)}`).join("&")}function lM(n,t){let{params:e,method:r,responseType:i}=n,o=c0(e),s=n.serializeBody();s instanceof URLSearchParams?s=c0(s):typeof s!="string"&&(s="");let a=[r,i,t,s,o].join("|"),c=dM(a);return c}function dM(n){let t=0;for(let e of n)t=Math.imul(31,t)+e.charCodeAt(0)<<0;return t+=2147483648,t.toString()}function l0(n){let t=atob(n);return Uint8Array.from(t,r=>r.charCodeAt(0)).buffer}var d0=(()=>{let n=Hl("json");return n.arrayBuffer=Hl("arraybuffer"),n.blob=Hl("blob"),n.text=Hl("text"),n})();function Hl(n){return function(e,r){let i=r?.injector??f(V),o=i.get(rM,null,{optional:!0}),s=i.get(Im,null,{optional:!0}),a=i.get(KT,null,{optional:!0}),c=l=>{if(o&&s&&l){let d=aM(l,o,s,a);if(d)try{let u=d.body,m=r?.parse?r.parse(u):u;return X({value:m})}catch{}}};return new Fh(i,()=>uM(e,n),r?.defaultValue,r?.debugName,r?.parse,r?.equal,c)}}function uM(n,t){let e=typeof n=="function"?n():n;if(e===void 0)return;typeof e=="string"&&(e={url:e});let r=e.headers instanceof Dn?e.headers:new Dn(e.headers),i=e.params instanceof xn?e.params:new xn({fromObject:e.params});return new Oi(e.method??"GET",e.url,e.body??null,{headers:r,params:i,reportProgress:e.reportProgress,withCredentials:e.withCredentials,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,responseType:t,context:e.context,transferCache:e.transferCache,credentials:e.credentials,referrer:e.referrer,referrerPolicy:e.referrerPolicy,integrity:e.integrity,timeout:e.timeout})}var Fh=class extends Mo{client;_headers=St({source:this.extRequest,computation:()=>{}});_progress=St({source:this.extRequest,computation:()=>{}});_statusCode=St({source:this.extRequest,computation:()=>{}});headers=Me(()=>this.status()==="resolved"||this.status()==="error"?this._headers():void 0);progress=this._progress.asReadonly();statusCode=this._statusCode.asReadonly();constructor(t,e,r,i,o,s,a){super(e,({params:c,abortSignal:l})=>{let d,u=()=>d.unsubscribe();l.addEventListener("abort",u);let m=X({value:void 0}),h,p=new Promise(y=>h=y),g=y=>{m.set(y),h?.(m),h=void 0};return d=this.client.request(c).subscribe({next:y=>{switch(y.type){case lr.Response:this._headers.set(y.headers),this._statusCode.set(y.status);try{g({value:o?o(y.body):y.body})}catch(D){g({error:Bs(D)})}break;case lr.DownloadProgress:this._progress.set(y);break}},error:y=>{y instanceof Ni&&(this._headers.set(y.headers),this._statusCode.set(y.status)),g({error:y}),l.removeEventListener("abort",u)},complete:()=>{h&&g({error:new M(991,!1)}),l.removeEventListener("abort",u)}}),p},r,s,i,t,a),this.client=t.get(a0)}set(t){super.set(t),this._headers.set(void 0),this._progress.set(void 0),this._statusCode.set(void 0)}};var Lh=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:function(r){let i=null;return r?i=new(r||n):i=K(fM),i},providedIn:"root"})}return n})(),fM=(()=>{class n extends Lh{_doc;constructor(e){super(),this._doc=e}sanitize(e,r){if(r==null)return null;switch(e){case Nt.NONE:return r;case Nt.HTML:return ir(r,"HTML")?bn(r):dl(this._doc,String(r)).toString();case Nt.STYLE:return ir(r,"Style")?bn(r):r;case Nt.SCRIPT:if(ir(r,"Script"))return bn(r);throw new M(5200,!1);case Nt.URL:return ir(r,"URL")?bn(r):Rs(String(r));case Nt.RESOURCE_URL:if(ir(r,"ResourceURL"))return bn(r);throw new M(5201,!1);default:throw new M(5202,!1)}}bypassSecurityTrustHtml(e){return Tm(e)}bypassSecurityTrustStyle(e){return Mm(e)}bypassSecurityTrustScript(e){return Am(e)}bypassSecurityTrustUrl(e){return Rm(e)}bypassSecurityTrustResourceUrl(e){return Om(e)}static \u0275fac=function(r){return new(r||n)(K(B))};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Ws(n){return n.buttons===0||n.detail===0}function qs(n){let t=n.touches&&n.touches[0]||n.changedTouches&&n.changedTouches[0];return!!t&&t.identifier===-1&&(t.radiusX==null||t.radiusX===1)&&(t.radiusY==null||t.radiusY===1)}var Bh;function u0(){if(Bh==null){let n=typeof document<"u"?document.head:null;Bh=!!(n&&(n.createShadowRoot||n.attachShadow))}return Bh}function Vh(n){if(u0()){let t=n.getRootNode?n.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&t instanceof ShadowRoot)return t}return null}function Ys(){let n=typeof document<"u"&&document?document.activeElement:null;for(;n&&n.shadowRoot;){let t=n.shadowRoot.activeElement;if(t===n)break;n=t}return n}function mt(n){return n.composedPath?n.composedPath()[0]:n.target}var jh;try{jh=typeof Intl<"u"&&Intl.v8BreakIterator}catch{jh=!1}var Te=(()=>{class n{_platformId=f(wi);isBrowser=this._platformId?$v(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||jh)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Zs;function f0(){if(Zs==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Zs=!0}))}finally{Zs=Zs||!1}return Zs}function Ro(n){return f0()?n:!!n.capture}function Ks(n,t=0){return m0(n)?Number(n):arguments.length===2?t:0}function m0(n){return!isNaN(parseFloat(n))&&!isNaN(Number(n))}function zn(n){return n instanceof z?n.nativeElement:n}var h0=new x("cdk-input-modality-detector-options"),p0={ignoreKeys:[18,17,224,91,16]},g0=650,Hh={passive:!0,capture:!0},b0=(()=>{class n{_platform=f(Te);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Qr(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(r=>r===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=mt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<g0||(this._modality.next(Ws(e)?"keyboard":"mouse"),this._mostRecentTarget=mt(e))};_onTouchstart=e=>{if(qs(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=mt(e)};constructor(){let e=f(F),r=f(B),i=f(h0,{optional:!0});if(this._options=w(w({},p0),i),this.modalityDetected=this._modality.pipe(rs(1)),this.modalityChanged=this.modalityDetected.pipe(dc()),this._platform.isBrowser){let o=f(ct).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(r,"keydown",this._onKeydown,Hh),o.listen(r,"mousedown",this._onMousedown,Hh),o.listen(r,"touchstart",this._onTouchstart,Hh)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Qs=(function(n){return n[n.IMMEDIATE=0]="IMMEDIATE",n[n.EVENTUAL=1]="EVENTUAL",n})(Qs||{}),y0=new x("cdk-focus-monitor-default-options"),zl=Ro({passive:!0,capture:!0}),Pi=(()=>{class n{_ngZone=f(F);_platform=f(Te);_inputModalityDetector=f(b0);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=f(B);_stopInputModalityDetector=new E;constructor(){let e=f(y0,{optional:!0});this._detectionMode=e?.detectionMode||Qs.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let r=mt(e);for(let i=r;i;i=i.parentElement)e.type==="focus"?this._onFocus(e,i):this._onBlur(e,i)};monitor(e,r=!1){let i=zn(e);if(!this._platform.isBrowser||i.nodeType!==1)return An();let o=Vh(i)||this._document,s=this._elementInfo.get(i);if(s)return r&&(s.checkChildren=!0),s.subject;let a={checkChildren:r,subject:new E,rootNode:o};return this._elementInfo.set(i,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let r=zn(e),i=this._elementInfo.get(r);i&&(i.subject.complete(),this._setClasses(r),this._elementInfo.delete(r),this._removeGlobalListeners(i))}focusVia(e,r,i){let o=zn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,r,c)):(this._setOrigin(r),typeof o.focus=="function"&&o.focus(i))}ngOnDestroy(){this._elementInfo.forEach((e,r)=>this.stopMonitoring(r))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Qs.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,r){e.classList.toggle("cdk-focused",!!r),e.classList.toggle("cdk-touch-focused",r==="touch"),e.classList.toggle("cdk-keyboard-focused",r==="keyboard"),e.classList.toggle("cdk-mouse-focused",r==="mouse"),e.classList.toggle("cdk-program-focused",r==="program")}_setOrigin(e,r=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&r,this._detectionMode===Qs.IMMEDIATE){clearTimeout(this._originTimeoutId);let i=this._originFromTouchInteraction?g0:1;this._originTimeoutId=setTimeout(()=>this._origin=null,i)}})}_onFocus(e,r){let i=this._elementInfo.get(r),o=mt(e);!i||!i.checkChildren&&r!==o||this._originChanged(r,this._getFocusOrigin(o),i)}_onBlur(e,r){let i=this._elementInfo.get(r);!i||i.checkChildren&&e.relatedTarget instanceof Node&&r.contains(e.relatedTarget)||(this._setClasses(r),this._emitOrigin(i,null))}_emitOrigin(e,r){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(r))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let r=e.rootNode,i=this._rootNodeFocusListenerCount.get(r)||0;i||this._ngZone.runOutsideAngular(()=>{r.addEventListener("focus",this._rootNodeFocusAndBlurListener,zl),r.addEventListener("blur",this._rootNodeFocusAndBlurListener,zl)}),this._rootNodeFocusListenerCount.set(r,i+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(st(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let r=e.rootNode;if(this._rootNodeFocusListenerCount.has(r)){let i=this._rootNodeFocusListenerCount.get(r);i>1?this._rootNodeFocusListenerCount.set(r,i-1):(r.removeEventListener("focus",this._rootNodeFocusAndBlurListener,zl),r.removeEventListener("blur",this._rootNodeFocusAndBlurListener,zl),this._rootNodeFocusListenerCount.delete(r))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,r,i){this._setClasses(e,r),this._emitOrigin(i,r),this._lastFocusOrigin=r}_getClosestElementsInfo(e){let r=[];return this._elementInfo.forEach((i,o)=>{(o===e||i.checkChildren&&o.contains(e))&&r.push([o,i])}),r}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:r,mostRecentModality:i}=this._inputModalityDetector;if(i!=="mouse"||!r||r===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(r))return!0}return!1}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Ul=new WeakMap,vt=(()=>{class n{_appRef;_injector=f(V);_environmentInjector=f(qe);load(e){let r=this._appRef=this._appRef||this._injector.get(Qt),i=Ul.get(r);i||(i={loaders:new Set,refs:[]},Ul.set(r,i),r.onDestroy(()=>{Ul.get(r)?.refs.forEach(o=>o.destroy()),Ul.delete(r)})),i.loaders.has(e)||(i.loaders.add(e),i.refs.push(Ml(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Gl=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(r,i){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return n})(),$l;function mM(){if($l===void 0&&($l=null,typeof window<"u")){let n=window;n.trustedTypes!==void 0&&($l=n.trustedTypes.createPolicy("angular#components",{createHTML:t=>t}))}return $l}function hM(n){return mM()?.createHTML(n)||n}function _0(n,t,e){let r=e.sanitize(Nt.HTML,t);n.innerHTML=hM(r||"")}function Oo(n){return Array.isArray(n)?n:[n]}var v0=new Set,Fi,Wl=(()=>{class n{_platform=f(Te);_nonce=f(Ci,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):gM}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&pM(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function pM(n,t){if(!v0.has(n))try{Fi||(Fi=document.createElement("style"),t&&Fi.setAttribute("nonce",t),Fi.setAttribute("type","text/css"),document.head.appendChild(Fi)),Fi.sheet&&(Fi.sheet.insertRule(`@media ${n} {body{ }}`,0),v0.add(n))}catch(e){console.error(e)}}function gM(n){return{matches:n==="all"||n==="",media:n,addListener:()=>{},removeListener:()=>{}}}var zh=(()=>{class n{_mediaMatcher=f(Wl);_zone=f(F);_queries=new Map;_destroySubject=new E;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return x0(Oo(e)).some(i=>this._registerQuery(i).mql.matches)}observe(e){let i=x0(Oo(e)).map(s=>this._registerQuery(s).observable),o=pu(i);return o=io(o.pipe(Mt(1)),o.pipe(rs(1),ns(0))),o.pipe(Le(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:c,query:l})=>{a.matches=a.matches||c,a.breakpoints[l]=c}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let r=this._mediaMatcher.matchMedia(e),o={observable:new le(s=>{let a=c=>this._zone.run(()=>s.next(c));return r.addListener(a),()=>{r.removeListener(a)}}).pipe(pt(r),Le(({matches:s})=>({query:e,matches:s})),st(this._destroySubject)),mql:r};return this._queries.set(e,o),o}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function x0(n){return n.map(t=>t.split(",")).reduce((t,e)=>t.concat(e)).map(t=>t.trim())}var $h=(()=>{class n{_platform=f(Te);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return yM(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let r=bM(kM(e));if(r&&(D0(r)===-1||!this.isVisible(r)))return!1;let i=e.nodeName.toLowerCase(),o=D0(e);return e.hasAttribute("contenteditable")?o!==-1:i==="iframe"||i==="object"||this._platform.WEBKIT&&this._platform.IOS&&!CM(e)?!1:i==="audio"?e.hasAttribute("controls")?o!==-1:!1:i==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,r){return EM(e)&&!this.isDisabled(e)&&(r?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function bM(n){try{return n.frameElement}catch{return null}}function yM(n){return!!(n.offsetWidth||n.offsetHeight||typeof n.getClientRects=="function"&&n.getClientRects().length)}function _M(n){let t=n.nodeName.toLowerCase();return t==="input"||t==="select"||t==="button"||t==="textarea"}function vM(n){return DM(n)&&n.type=="hidden"}function xM(n){return wM(n)&&n.hasAttribute("href")}function DM(n){return n.nodeName.toLowerCase()=="input"}function wM(n){return n.nodeName.toLowerCase()=="a"}function w0(n){if(!n.hasAttribute("tabindex")||n.tabIndex===void 0)return!1;let t=n.getAttribute("tabindex");return!!(t&&!isNaN(parseInt(t,10)))}function D0(n){if(!w0(n))return null;let t=parseInt(n.getAttribute("tabindex")||"",10);return isNaN(t)?-1:t}function CM(n){let t=n.nodeName.toLowerCase(),e=t==="input"&&n.type;return e==="text"||e==="password"||t==="select"||t==="textarea"}function EM(n){return vM(n)?!1:_M(n)||xM(n)||n.hasAttribute("contenteditable")||w0(n)}function kM(n){return n.ownerDocument&&n.ownerDocument.defaultView||window}var Uh=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(t){this._enabled=t,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(t,this._startAnchor),this._toggleAnchorTabIndex(t,this._endAnchor))}_enabled=!0;constructor(t,e,r,i,o=!1,s){this._element=t,this._checker=e,this._ngZone=r,this._document=i,this._injector=s,o||this.attachAnchors()}destroy(){let t=this._startAnchor,e=this._endAnchor;t&&(t.removeEventListener("focus",this.startAnchorListener),t.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(t){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(t)))})}focusFirstTabbableElementWhenReady(t){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(t)))})}focusLastTabbableElementWhenReady(t){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(t)))})}_getRegionBoundary(t){let e=this._element.querySelectorAll(`[cdk-focus-region-${t}], [cdkFocusRegion${t}], [cdk-focus-${t}]`);return t=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(t){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let r=this._getFirstTabbableElement(e);return r?.focus(t),!!r}return e.focus(t),!0}return this.focusFirstTabbableElement(t)}focusFirstTabbableElement(t){let e=this._getRegionBoundary("start");return e&&e.focus(t),!!e}focusLastTabbableElement(t){let e=this._getRegionBoundary("end");return e&&e.focus(t),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(t){if(this._checker.isFocusable(t)&&this._checker.isTabbable(t))return t;let e=t.children;for(let r=0;r<e.length;r++){let i=e[r].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[r]):null;if(i)return i}return null}_getLastTabbableElement(t){if(this._checker.isFocusable(t)&&this._checker.isTabbable(t))return t;let e=t.children;for(let r=e.length-1;r>=0;r--){let i=e[r].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[r]):null;if(i)return i}return null}_createAnchor(){let t=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,t),t.classList.add("cdk-visually-hidden"),t.classList.add("cdk-focus-trap-anchor"),t.setAttribute("aria-hidden","true"),t}_toggleAnchorTabIndex(t,e){t?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(t){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(t,this._startAnchor),this._toggleAnchorTabIndex(t,this._endAnchor))}_executeOnStable(t){this._injector?Ft(t,{injector:this._injector}):setTimeout(t)}},Gh=(()=>{class n{_checker=f($h);_ngZone=f(F);_document=f(B);_injector=f(V);constructor(){f(vt).load(Gl)}create(e,r=!1){return new Uh(e,this._checker,this._ngZone,this._document,r,this._injector)}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var C0=new x("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),E0=new x("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),IM=0,Xs=(()=>{class n{_ngZone=f(F);_defaultOptions=f(E0,{optional:!0});_liveElement;_document=f(B);_sanitizer=f(Lh);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=f(C0,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...r){let i=this._defaultOptions,o,s;return r.length===1&&typeof r[0]=="number"?s=r[0]:[o,s]=r,this.clear(),clearTimeout(this._previousTimeout),o||(o=i&&i.politeness?i.politeness:"polite"),s==null&&i&&(s=i.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:_0(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",r=this._document.getElementsByClassName(e),i=this._document.createElement("div");for(let o=0;o<r.length;o++)r[o].remove();return i.classList.add(e),i.classList.add("cdk-visually-hidden"),i.setAttribute("aria-atomic","true"),i.setAttribute("aria-live","polite"),i.id=`cdk-live-announcer-${IM++}`,this._document.body.appendChild(i),i}_exposeAnnouncerToModals(e){let r=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let i=0;i<r.length;i++){let o=r[i],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var SM=200,ql=class{_letterKeyStream=new E;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new E;selectedItem=this._selectedItem;constructor(t,e){let r=typeof e?.debounceInterval=="number"?e.debounceInterval:SM;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(t),this._setupKeyHandler(r)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(t){this._selectedItemIndex=t}setItems(t){this._items=t}handleKey(t){let e=t.keyCode;t.key&&t.key.length===1?this._letterKeyStream.next(t.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(t){this._letterKeyStream.pipe(xu(e=>this._pressedLetters.push(e)),ns(t),Ie(()=>this._pressedLetters.length>0),Le(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let r=1;r<this._items.length+1;r++){let i=(this._selectedItemIndex+r)%this._items.length,o=this._items[i];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function xt(n,...t){return t.length?t.some(e=>n[e]):n.altKey||n.shiftKey||n.ctrlKey||n.metaKey}var No=class{_items;_activeItemIndex=X(-1);_activeItem=X(null);_wrap=!1;_typeaheadSubscription=ye.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=t=>t.disabled;constructor(t,e){this._items=t,t instanceof nr?this._itemChangesSubscription=t.changes.subscribe(r=>this._itemsChanged(r.toArray())):Nr(t)&&(this._effectRef=bt(()=>this._itemsChanged(t()),{injector:e}))}tabOut=new E;change=new E;skipPredicate(t){return this._skipPredicateFn=t,this}withWrap(t=!0){return this._wrap=t,this}withVerticalOrientation(t=!0){return this._vertical=t,this}withHorizontalOrientation(t){return this._horizontal=t,this}withAllowedModifierKeys(t){return this._allowedModifierKeys=t,this}withTypeAhead(t=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new ql(e,{debounceInterval:typeof t=="number"?t:void 0,skipPredicate:r=>this._skipPredicateFn(r)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(r=>{this.setActiveItem(r)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(t=!0){return this._homeAndEnd=t,this}withPageUpDown(t=!0,e=10){return this._pageUpAndDown={enabled:t,delta:e},this}setActiveItem(t){let e=this._activeItem();this.updateActiveItem(t),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(t){let e=t.keyCode,i=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!t[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&i){this.setNextItemActive();break}else return;case 38:if(this._vertical&&i){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&i){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&i){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&i){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&i){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&i){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&i){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(i||xt(t,"shiftKey"))&&this._typeahead?.handleKey(t);return}this._typeahead?.reset(),t.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(t){let e=this._getItemsArray(),r=typeof t=="number"?t:e.indexOf(t),i=e[r];this._activeItem.set(i??null),this._activeItemIndex.set(r),this._typeahead?.setCurrentSelectedItemIndex(r)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(t){this._wrap?this._setActiveInWrapMode(t):this._setActiveInDefaultMode(t)}_setActiveInWrapMode(t){let e=this._getItemsArray();for(let r=1;r<=e.length;r++){let i=(this._activeItemIndex()+t*r+e.length)%e.length,o=e[i];if(!this._skipPredicateFn(o)){this.setActiveItem(i);return}}}_setActiveInDefaultMode(t){this._setActiveItemByIndex(this._activeItemIndex()+t,t)}_setActiveItemByIndex(t,e){let r=this._getItemsArray();if(r[t]){for(;this._skipPredicateFn(r[t]);)if(t+=e,!r[t])return;this.setActiveItem(t)}}_getItemsArray(){return Nr(this._items)?this._items():this._items instanceof nr?this._items.toArray():this._items}_itemsChanged(t){this._typeahead?.setItems(t);let e=this._activeItem();if(e){let r=t.indexOf(e);r>-1&&r!==this._activeItemIndex()&&(this._activeItemIndex.set(r),this._typeahead?.setCurrentSelectedItemIndex(r))}}};var Js=class extends No{setActiveItem(t){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(t),this.activeItem&&this.activeItem.setActiveStyles()}};var ea=class extends No{_origin="program";setFocusOrigin(t){return this._origin=t,this}setActiveItem(t){super.setActiveItem(t),this.activeItem&&this.activeItem.focus(this._origin)}};var Yh={},Fe=class n{_appId=f(Di);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(t,e=!1){return this._appId!=="ng"&&(t+=this._appId),Yh.hasOwnProperty(t)||(Yh[t]=0),`${t}${e?n._infix+"-":""}${Yh[t]++}`}static \u0275fac=function(e){return new(e||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})};var T0=" ";function M0(n,t,e){let r=A0(n,t);e=e.trim(),!r.some(i=>i.trim()===e)&&(r.push(e),n.setAttribute(t,r.join(T0)))}function Zh(n,t,e){let r=A0(n,t);e=e.trim();let i=r.filter(o=>o!==e);i.length?n.setAttribute(t,i.join(T0)):n.removeAttribute(t)}function A0(n,t){return n.getAttribute(t)?.match(/\S+/g)??[]}var MM=new x("cdk-dir-doc",{providedIn:"root",factory:()=>f(B)}),AM=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function R0(n){let t=n?.toLowerCase()||"";return t==="auto"&&typeof navigator<"u"&&navigator?.language?AM.test(navigator.language)?"rtl":"ltr":t==="rtl"?"rtl":"ltr"}var wn=(()=>{class n{get value(){return this.valueSignal()}valueSignal=X("ltr");change=new j;constructor(){let e=f(MM,{optional:!0});if(e){let r=e.body?e.body.dir:null,i=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(R0(r||i||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Qe(n){return n==null?"":typeof n=="string"?n:`${n}px`}function ta(n){return n!=null&&`${n}`!="false"}var Li;function O0(){if(Li==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Li=!1,Li;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Li=!0;else{let n=Element.prototype.scrollTo;n?Li=!/\{\s*\[native code\]\s*\}/.test(n.toString()):Li=!1}}return Li}function Kh(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Qh=class{_box;_destroyed=new E;_resizeSubject=new E;_resizeObserver;_elementObservables=new Map;constructor(t){this._box=t,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(t){return this._elementObservables.has(t)||this._elementObservables.set(t,new le(e=>{let r=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(t,{box:this._box}),()=>{this._resizeObserver?.unobserve(t),r.unsubscribe(),this._elementObservables.delete(t)}}).pipe(Ie(e=>e.some(r=>r.target===t)),fc({bufferSize:1,refCount:!0}),st(this._destroyed))),this._elementObservables.get(t)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},N0=(()=>{class n{_cleanupErrorListener;_observers=new Map;_ngZone=f(F);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,r){let i=r?.box||"content-box";return this._observers.has(i)||this._observers.set(i,new Qh(i)),this._observers.get(i).observe(e)}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var P0={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var RM=new x("MATERIAL_ANIMATIONS"),F0=null;function Xh(){return f(RM,{optional:!0})?.animationsDisabled||f(As,{optional:!0})==="NoopAnimations"?"di-disabled":(F0??=f(Wl).matchMedia("(prefers-reduced-motion)").matches,F0?"reduced-motion":"enabled")}function Ge(){return Xh()!=="enabled"}var OM=["notch"],NM=["matFormFieldNotchedOutline",""],PM=["*"],L0=["iconPrefixContainer"],B0=["textPrefixContainer"],V0=["iconSuffixContainer"],j0=["textSuffixContainer"],FM=["textField"],LM=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],BM=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function VM(n,t){n&1&&fe(0,"span",21)}function jM(n,t){if(n&1&&(I(0,"label",20),ae(1,1),pe(2,VM,1,0,"span",21),T()),n&2){let e=ne(2);te("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),ue("for",e._control.disableAutomaticLabeling?null:e._control.id),S(2),ge(!e.hideRequiredMarker&&e._control.required?2:-1)}}function HM(n,t){if(n&1&&pe(0,jM,3,5,"label",20),n&2){let e=ne();ge(e._hasFloatingLabel()?0:-1)}}function zM(n,t){n&1&&fe(0,"div",7)}function UM(n,t){}function $M(n,t){if(n&1&&Ue(0,UM,0,0,"ng-template",13),n&2){ne(2);let e=Vt(1);te("ngTemplateOutlet",e)}}function GM(n,t){if(n&1&&(I(0,"div",9),pe(1,$M,1,1,null,13),T()),n&2){let e=ne();te("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),S(),ge(e._forceDisplayInfixLabel()?-1:1)}}function WM(n,t){n&1&&(I(0,"div",10,2),ae(2,2),T())}function qM(n,t){n&1&&(I(0,"div",11,3),ae(2,3),T())}function YM(n,t){}function ZM(n,t){if(n&1&&Ue(0,YM,0,0,"ng-template",13),n&2){ne();let e=Vt(1);te("ngTemplateOutlet",e)}}function KM(n,t){n&1&&(I(0,"div",14,4),ae(2,4),T())}function QM(n,t){n&1&&(I(0,"div",15,5),ae(2,5),T())}function XM(n,t){n&1&&fe(0,"div",16)}function JM(n,t){n&1&&(I(0,"div",18),ae(1,6),T())}function e1(n,t){if(n&1&&(I(0,"mat-hint",22),_t(1),T()),n&2){let e=ne(2);te("id",e._hintLabelId),S(),Ti(e.hintLabel)}}function t1(n,t){if(n&1&&(I(0,"div",19),pe(1,e1,2,2,"mat-hint",22),ae(2,7),fe(3,"div",23),ae(4,8),T()),n&2){let e=ne();S(),ge(e.hintLabel?1:-1)}}var Jh=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["mat-label"]]})}return n})(),q0=new x("MatError");var ep=(()=>{class n{align="start";id=f(Fe).getId("mat-mdc-hint-");static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(r,i){r&2&&(Hn("id",i.id),ue("align",null),Q("mat-mdc-form-field-hint-end",i.align==="end"))},inputs:{align:"align",id:"id"}})}return n})(),Y0=new x("MatPrefix");var Z0=new x("MatSuffix");var K0=new x("FloatingLabelParent"),H0=(()=>{class n{_elementRef=f(z);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=f(N0);_ngZone=f(F);_parent=f(K0);_resizeSubscription=new ye;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return n1(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(r,i){r&2&&Q("mdc-floating-label--float-above",i.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return n})();function n1(n){let t=n;if(t.offsetParent!==null)return t.scrollWidth;let e=t.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let r=e.scrollWidth;return e.remove(),r}var z0="mdc-line-ripple--active",Yl="mdc-line-ripple--deactivating",U0=(()=>{class n{_elementRef=f(z);_cleanupTransitionEnd;constructor(){let e=f(F),r=f(lt);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=r.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Yl),e.add(z0)}deactivate(){this._elementRef.nativeElement.classList.add(Yl)}_handleTransitionEnd=e=>{let r=this._elementRef.nativeElement.classList,i=r.contains(Yl);e.propertyName==="opacity"&&i&&r.remove(z0,Yl)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return n})(),$0=(()=>{class n{_elementRef=f(z);_ngZone=f(F);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,r=e.querySelector(".mdc-floating-label");r?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(r.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>r.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let r=this._notch.nativeElement;!this.open||!e?r.style.width="":r.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(r,i){if(r&1&&et(OM,5),r&2){let o;J(o=ee())&&(i._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(r,i){r&2&&Q("mdc-notched-outline--notched",i.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:NM,ngContentSelectors:PM,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(r,i){r&1&&(Ze(),Jt(0,"div",1),Bt(1,"div",2,0),ae(3),Xt(),Jt(4,"div",3))},encapsulation:2,changeDetection:0})}return n})(),Zl=(()=>{class n{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n})}return n})();var Kl=new x("MatFormField"),Ql=new x("MAT_FORM_FIELD_DEFAULT_OPTIONS"),G0="fill",r1="auto",W0="fixed",i1="translateY(-50%)",tp=(()=>{class n{_elementRef=f(z);_changeDetectorRef=f(tt);_platform=f(Te);_idGenerator=f(Fe);_ngZone=f(F);_defaults=f(Ql,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Mi("iconPrefixContainer");_textPrefixContainerSignal=Mi("textPrefixContainer");_iconSuffixContainerSignal=Mi("iconSuffixContainer");_textSuffixContainerSignal=Mi("textSuffixContainer");_prefixSuffixContainers=Me(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Pv(Jh);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=ta(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||r1}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let r=e||this._defaults?.appearance||G0;this._appearanceSignal.set(r)}_appearanceSignal=X(G0);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||W0}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||W0}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new E;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ge();constructor(){let e=this._defaults,r=f(wn);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),bt(()=>this._currentDirection=r.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Me(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let r=this._control,i="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(i+e.controlType),r.controlType&&this._elementRef.nativeElement.classList.add(i+r.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=r.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=r.stateChanges.pipe(pt([void 0,void 0]),Le(()=>[r.errorState,r.userAriaDescribedBy]),uc(),Ie(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),r.ngControl&&r.ngControl.valueChanges&&(this._valueChanges=r.ngControl.valueChanges.pipe(st(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),an(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){Bv({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Me(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let r=this._control?this._control.ngControl:null;return r&&r[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let r=this._control.describedByIds,i;if(r){let o=this._describedByIds||e;i=e.concat(r.filter(s=>s&&!o.includes(s)))}else i=e;this._control.setDescribedByIds(i),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,r=this._textPrefixContainer?.nativeElement,i=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=r?.getBoundingClientRect().width??0,c=i?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${s+a}px`,h=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,p=`var(--mat-mdc-form-field-label-transform, ${i1} translateX(${h}))`,g=s+a+c+l;return[p,g]}_writeOutlinedLabelStyles(e){if(e!==null){let[r,i]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=r),i!==null&&this._notchedOutline?._setMaxWidth(i)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let r=e.getRootNode();return r&&r!==e}return document.documentElement.contains(e)}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-form-field"]],contentQueries:function(r,i,o){if(r&1&&(Dl(o,i._labelChild,Jh,5),_n(o,Zl,5)(o,Y0,5)(o,Z0,5)(o,q0,5)(o,ep,5)),r&2){To();let s;J(s=ee())&&(i._formFieldControl=s.first),J(s=ee())&&(i._prefixChildren=s),J(s=ee())&&(i._suffixChildren=s),J(s=ee())&&(i._errorChildren=s),J(s=ee())&&(i._hintChildren=s)}},viewQuery:function(r,i){if(r&1&&(So(i._iconPrefixContainerSignal,L0,5)(i._textPrefixContainerSignal,B0,5)(i._iconSuffixContainerSignal,V0,5)(i._textSuffixContainerSignal,j0,5),et(FM,5)(L0,5)(B0,5)(V0,5)(j0,5)(H0,5)($0,5)(U0,5)),r&2){To(4);let o;J(o=ee())&&(i._textField=o.first),J(o=ee())&&(i._iconPrefixContainer=o.first),J(o=ee())&&(i._textPrefixContainer=o.first),J(o=ee())&&(i._iconSuffixContainer=o.first),J(o=ee())&&(i._textSuffixContainer=o.first),J(o=ee())&&(i._floatingLabel=o.first),J(o=ee())&&(i._notchedOutline=o.first),J(o=ee())&&(i._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(r,i){r&2&&Q("mat-mdc-form-field-label-always-float",i._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",i._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",i._hasIconSuffix)("mat-form-field-invalid",i._control.errorState)("mat-form-field-disabled",i._control.disabled)("mat-form-field-autofilled",i._control.autofilled)("mat-form-field-appearance-fill",i.appearance=="fill")("mat-form-field-appearance-outline",i.appearance=="outline")("mat-form-field-hide-placeholder",i._hasFloatingLabel()&&!i._shouldLabelFloat())("mat-primary",i.color!=="accent"&&i.color!=="warn")("mat-accent",i.color==="accent")("mat-warn",i.color==="warn")("ng-untouched",i._shouldForward("untouched"))("ng-touched",i._shouldForward("touched"))("ng-pristine",i._shouldForward("pristine"))("ng-dirty",i._shouldForward("dirty"))("ng-valid",i._shouldForward("valid"))("ng-invalid",i._shouldForward("invalid"))("ng-pending",i._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Ke([{provide:Kl,useExisting:n},{provide:K0,useExisting:n}])],ngContentSelectors:BM,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(r,i){if(r&1&&(Ze(LM),Ue(0,HM,1,1,"ng-template",null,0,Pr),I(2,"div",6,1),Ce("click",function(s){return i._control.onContainerClick(s)}),pe(4,zM,1,0,"div",7),I(5,"div",8),pe(6,GM,2,2,"div",9),pe(7,WM,3,0,"div",10),pe(8,qM,3,0,"div",11),I(9,"div",12),pe(10,ZM,1,1,null,13),ae(11),T(),pe(12,KM,3,0,"div",14),pe(13,QM,3,0,"div",15),T(),pe(14,XM,1,0,"div",16),T(),I(15,"div",17),pe(16,JM,2,0,"div",18)(17,t1,5,1,"div",19),T()),r&2){let o;S(2),Q("mdc-text-field--filled",!i._hasOutline())("mdc-text-field--outlined",i._hasOutline())("mdc-text-field--no-label",!i._hasFloatingLabel())("mdc-text-field--disabled",i._control.disabled)("mdc-text-field--invalid",i._control.errorState),S(2),ge(!i._hasOutline()&&!i._control.disabled?4:-1),S(2),ge(i._hasOutline()?6:-1),S(),ge(i._hasIconPrefix?7:-1),S(),ge(i._hasTextPrefix?8:-1),S(2),ge(!i._hasOutline()||i._forceDisplayInfixLabel()?10:-1),S(2),ge(i._hasTextSuffix?12:-1),S(),ge(i._hasIconSuffix?13:-1),S(),ge(i._hasOutline()?-1:14),S(),Q("mat-mdc-form-field-subscript-dynamic-size",i.subscriptSizing==="dynamic");let s=i._getSubscriptMessageType();S(),ge((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[H0,$0,Ai,U0,ep],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return n})();var o1=20,Q0=(()=>{class n{_ngZone=f(F);_platform=f(Te);_renderer=f(ct).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new E;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let r=this.scrollContainers.get(e);r&&(r.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=o1){return this._platform.isBrowser?new le(r=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let i=e>0?this._scrolled.pipe(lc(e)).subscribe(r):this._scrolled.subscribe(r);return this._scrolledCount++,()=>{i.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):An()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,r)=>this.deregister(r)),this._scrolled.complete()}ancestorScrolled(e,r){let i=this.getAncestorScrollContainers(e);return this.scrolled(r).pipe(Ie(o=>!o||i.indexOf(o)>-1))}getAncestorScrollContainers(e){let r=[];return this.scrollContainers.forEach((i,o)=>{this._scrollableContainsElement(o,e)&&r.push(o)}),r}_scrollableContainsElement(e,r){let i=zn(r),o=e.getElementRef().nativeElement;do if(i==o)return!0;while(i=i.parentElement);return!1}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var s1=20,Po=(()=>{class n{_platform=f(Te);_listeners;_viewportSize=null;_change=new E;_document=f(B);constructor(){let e=f(F),r=f(ct).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let i=o=>this._change.next(o);this._listeners=[r.listen("window","resize",i),r.listen("window","orientationchange",i)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:r,height:i}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+i,right:e.left+r,height:i,width:r}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,r=this._getWindow(),i=e.documentElement,o=i.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||r.scrollY||i.scrollTop||0,a=-o.left||e.body?.scrollLeft||r.scrollX||i.scrollLeft||0;return{top:s,left:a}}change(e=s1){return e>0?this._change.pipe(lc(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var na=class{_attachedHost=null;attach(t){return this._attachedHost=t,t.attach(this)}detach(){let t=this._attachedHost;t!=null&&(this._attachedHost=null,t.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(t){this._attachedHost=t}},dr=class extends na{component;viewContainerRef;injector;projectableNodes;bindings;constructor(t,e,r,i,o){super(),this.component=t,this.viewContainerRef=e,this.injector=r,this.projectableNodes=i,this.bindings=o||null}},Cn=class extends na{templateRef;viewContainerRef;context;injector;constructor(t,e,r,i){super(),this.templateRef=t,this.viewContainerRef=e,this.context=r,this.injector=i}get origin(){return this.templateRef.elementRef}attach(t,e=this.context){return this.context=e,super.attach(t)}detach(){return this.context=void 0,super.detach()}},np=class extends na{element;constructor(t){super(),this.element=t instanceof z?t.nativeElement:t}},Br=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(t){if(t instanceof dr)return this._attachedPortal=t,this.attachComponentPortal(t);if(t instanceof Cn)return this._attachedPortal=t,this.attachTemplatePortal(t);if(this.attachDomPortal&&t instanceof np)return this._attachedPortal=t,this.attachDomPortal(t)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(t){this._disposeFn=t}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Xl=class extends Br{outletElement;_appRef;_defaultInjector;constructor(t,e,r){super(),this.outletElement=t,this._appRef=e,this._defaultInjector=r}attachComponentPortal(t){let e;if(t.viewContainerRef){let r=t.injector||t.viewContainerRef.injector,i=r.get(Or,null,{optional:!0})||void 0;e=t.viewContainerRef.createComponent(t.component,{index:t.viewContainerRef.length,injector:r,ngModuleRef:i,projectableNodes:t.projectableNodes||void 0,bindings:t.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let r=this._appRef,i=t.injector||this._defaultInjector||V.NULL,o=i.get(qe,r.injector);e=Ml(t.component,{elementInjector:i,environmentInjector:o,projectableNodes:t.projectableNodes||void 0,bindings:t.bindings||void 0}),r.attachView(e.hostView),this.setDisposeFn(()=>{r.viewCount>0&&r.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=t,e}attachTemplatePortal(t){let e=t.viewContainerRef,r=e.createEmbeddedView(t.templateRef,t.context,{injector:t.injector});return r.rootNodes.forEach(i=>this.outletElement.appendChild(i)),r.detectChanges(),this.setDisposeFn(()=>{let i=e.indexOf(r);i!==-1&&e.remove(i)}),this._attachedPortal=t,r}attachDomPortal=t=>{let e=t.element;e.parentNode;let r=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(r,e),this.outletElement.appendChild(e),this._attachedPortal=t,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(e,r)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(t){return t.hostView.rootNodes[0]}};var Un=(()=>{class n extends Br{_moduleRef=f(Or,{optional:!0});_document=f(B);_viewContainerRef=f(Lt);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new j;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let r=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,i=r.createComponent(e.component,{index:r.length,injector:e.injector||r.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return r!==this._viewContainerRef&&this._getRootNode().appendChild(i.hostView.rootNodes[0]),super.setDisposeFn(()=>i.destroy()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachTemplatePortal(e){e.setAttachedHost(this);let r=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachDomPortal=e=>{let r=e.element;r.parentNode;let i=this._document.createComment("dom-portal");e.setAttachedHost(this),r.parentNode.insertBefore(i,r),this._getRootNode().appendChild(r),this._attachedPortal=e,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(r,i)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[Pe]})}return n})();var X0=O0();function ia(n){return new Jl(n.get(Po),n.get(B))}var Jl=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(t,e){this._viewportRuler=t,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let t=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=t.style.left||"",this._previousHTMLStyles.top=t.style.top||"",t.style.left=Qe(-this._previousScrollPosition.left),t.style.top=Qe(-this._previousScrollPosition.top),t.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let t=this._document.documentElement,e=this._document.body,r=t.style,i=e.style,o=r.scrollBehavior||"",s=i.scrollBehavior||"";this._isEnabled=!1,r.left=this._previousHTMLStyles.left,r.top=this._previousHTMLStyles.top,t.classList.remove("cdk-global-scrollblock"),X0&&(r.scrollBehavior=i.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),X0&&(r.scrollBehavior=o,i.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,r=this._viewportRuler.getViewportSize();return e.scrollHeight>r.height||e.scrollWidth>r.width}};var ed=class{enable(){}disable(){}attach(){}};function rp(n,t){return t.some(e=>{let r=n.bottom<e.top,i=n.top>e.bottom,o=n.right<e.left,s=n.left>e.right;return r||i||o||s})}function J0(n,t){return t.some(e=>{let r=n.top<e.top,i=n.bottom>e.bottom,o=n.left<e.left,s=n.right>e.right;return r||i||o||s})}function sd(n,t){return new td(n.get(Q0),n.get(Po),n.get(F),t)}var td=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(t,e,r,i){this._scrollDispatcher=t,this._viewportRuler=e,this._ngZone=r,this._config=i}attach(t){this._overlayRef,this._overlayRef=t}enable(){if(!this._scrollSubscription){let t=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(t).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:r,height:i}=this._viewportRuler.getViewportSize();rp(e,[{width:r,height:i,bottom:i,right:r,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}};var ur=class{positionStrategy;scrollStrategy=new ed;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(t){if(t){let e=Object.keys(t);for(let r of e)t[r]!==void 0&&(this[r]=t[r])}}};var nd=class{connectionPair;scrollableViewProperties;constructor(t,e){this.connectionPair=t,this.scrollableViewProperties=e}};var ox=(()=>{class n{_attachedOverlays=[];_document=f(B);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let r=this._attachedOverlays.indexOf(e);r>-1&&this._attachedOverlays.splice(r,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,r,i){return i.observers.length<1?!1:e.eventPredicate?e.eventPredicate(r):!0}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),sx=(()=>{class n extends ox{_ngZone=f(F);_renderer=f(ct).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let r=this._attachedOverlays;for(let i=r.length-1;i>-1;i--){let o=r[i];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(i){return(e||(e=Kt(n)))(i||n)}})();static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),ax=(()=>{class n extends ox{_platform=f(Te);_ngZone=f(F);_renderer=f(ct).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let r=this._document.body,i={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(r,"pointerdown",this._pointerDownListener,i),o.listen(r,"click",this._clickListener,i),o.listen(r,"auxclick",this._clickListener,i),o.listen(r,"contextmenu",this._clickListener,i)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=r.style.cursor,r.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=mt(e)};_clickListener=e=>{let r=mt(e),i=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:r;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,c))){if(ex(a.overlayElement,r)||ex(a.overlayElement,i))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=(()=>{let e;return function(i){return(e||(e=Kt(n)))(i||n)}})();static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function ex(n,t){let e=typeof ShadowRoot<"u"&&ShadowRoot,r=t;for(;r;){if(r===n)return!0;r=e&&r instanceof ShadowRoot?r.host:r.parentNode}return!1}var cx=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(r,i){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return n})(),ad=(()=>{class n{_platform=f(Te);_containerElement;_document=f(B);_styleLoader=f(vt);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Kh()){let i=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<i.length;o++)i[o].remove()}let r=this._document.createElement("div");r.classList.add(e),Kh()?r.setAttribute("platform","test"):this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._containerElement=r}_loadStyles(){this._styleLoader.load(cx)}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),ip=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(t,e,r,i){this._renderer=e,this._ngZone=r,this.element=t.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",i)}detach(){this._ngZone.runOutsideAngular(()=>{let t=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(t,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),t.style.pointerEvents="none",t.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function op(n){return n&&n.nodeType===1}var Fo=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new E;_attachments=new E;_detachments=new E;_positionStrategy;_scrollStrategy;_locationChanges=ye.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new E;_outsidePointerEvents=new E;_afterNextRenderRef;constructor(t,e,r,i,o,s,a,c,l,d=!1,u,m){this._portalOutlet=t,this._host=e,this._pane=r,this._config=i,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=u,this._renderer=m,i.scrollStrategy&&(this._scrollStrategy=i.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=i.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(t){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(t);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Ft(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let t=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),t}dispose(){if(this._disposed)return;let t=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,t&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(t){t!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=t,this.hasAttached()&&(t.attach(this),this.updatePosition()))}updateSize(t){this._config=w(w({},this._config),t),this._updateElementSize()}setDirection(t){this._config=W(w({},this._config),{direction:t}),this._updateElementDirection()}addPanelClass(t){this._pane&&this._toggleClasses(this._pane,t,!0)}removePanelClass(t){this._pane&&this._toggleClasses(this._pane,t,!1)}getDirection(){let t=this._config.direction;return t?typeof t=="string"?t:t.value:"ltr"}updateScrollStrategy(t){t!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=t,this.hasAttached()&&(t.attach(this),t.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let t=this._pane.style;t.width=Qe(this._config.width),t.height=Qe(this._config.height),t.minWidth=Qe(this._config.minWidth),t.minHeight=Qe(this._config.minHeight),t.maxWidth=Qe(this._config.maxWidth),t.maxHeight=Qe(this._config.maxHeight)}_togglePointerEvents(t){this._pane.style.pointerEvents=t?"":"none"}_attachHost(){if(!this._host.parentElement){let t=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;op(t)?t.after(this._host):t?.type==="parent"?t.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let t="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new ip(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(t))}):this._backdropRef.element.classList.add(t)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(t,e,r){let i=Oo(e||[]).filter(o=>!!o);i.length&&(r?t.classList.add(...i):t.classList.remove(...i))}_detachContentWhenEmpty(){let t=!1;try{this._detachContentAfterRenderRef=Ft(()=>{t=!0,this._detachContent()},{injector:this._injector})}catch(e){if(t)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let t=this._scrollStrategy;t?.disable(),t?.detach?.()}},tx="cdk-overlay-connected-position-bounding-box",a1=/([A-Za-z%]+)$/;function lx(n,t){return new rd(t,n.get(Po),n.get(B),n.get(Te),n.get(ad))}var rd=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new E;_resizeSubscription=ye.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(t,e,r,i,o){this._viewportRuler=e,this._document=r,this._platform=i,this._overlayContainer=o,this.setOrigin(t)}attach(t){this._overlayRef&&this._overlayRef,this._validatePositions(),t.hostElement.classList.add(tx),this._overlayRef=t,this._boundingBox=t.hostElement,this._pane=t.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let t=this._originRect,e=this._overlayRect,r=this._viewportRect,i=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(t,i,a),l=this._getOverlayPoint(c,e,a),d=this._getOverlayFit(l,e,r,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(d,l,r)){o.push({position:a,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:l,originPoint:c,position:a,overlayRect:e})}if(o.length){let a=null,c=-1;for(let l of o){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&Bi(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(tx),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let t=this._lastPosition;t?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(t,this._getOriginPoint(this._originRect,this._containerRect,t))):this.apply()}withScrollableContainers(t){return this._scrollables=t,this}withPositions(t){return this._preferredPositions=t,t.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(t){return this._viewportMargin=t,this}withFlexibleDimensions(t=!0){return this._hasFlexibleDimensions=t,this}withGrowAfterOpen(t=!0){return this._growAfterOpen=t,this}withPush(t=!0){return this._canPush=t,this}withLockedPosition(t=!0){return this._positionLocked=t,this}setOrigin(t){return this._origin=t,this}withDefaultOffsetX(t){return this._offsetX=t,this}withDefaultOffsetY(t){return this._offsetY=t,this}withTransformOriginOn(t){return this._transformOriginSelector=t,this}withPopoverLocation(t){return this._popoverLocation=t,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof z?this._origin.nativeElement:op(this._origin)?this._origin:null}_getOriginPoint(t,e,r){let i;if(r.originX=="center")i=t.left+t.width/2;else{let s=this._isRtl()?t.right:t.left,a=this._isRtl()?t.left:t.right;i=r.originX=="start"?s:a}e.left<0&&(i-=e.left);let o;return r.originY=="center"?o=t.top+t.height/2:o=r.originY=="top"?t.top:t.bottom,e.top<0&&(o-=e.top),{x:i,y:o}}_getOverlayPoint(t,e,r){let i;r.overlayX=="center"?i=-e.width/2:r.overlayX==="start"?i=this._isRtl()?-e.width:0:i=this._isRtl()?0:-e.width;let o;return r.overlayY=="center"?o=-e.height/2:o=r.overlayY=="top"?0:-e.height,{x:t.x+i,y:t.y+o}}_getOverlayFit(t,e,r,i){let o=rx(e),{x:s,y:a}=t,c=this._getOffset(i,"x"),l=this._getOffset(i,"y");c&&(s+=c),l&&(a+=l);let d=0-s,u=s+o.width-r.width,m=0-a,h=a+o.height-r.height,p=this._subtractOverflows(o.width,d,u),g=this._subtractOverflows(o.height,m,h),y=p*g;return{visibleArea:y,isCompletelyWithinViewport:o.width*o.height===y,fitsInViewportVertically:g===o.height,fitsInViewportHorizontally:p==o.width}}_canFitWithFlexibleDimensions(t,e,r){if(this._hasFlexibleDimensions){let i=r.bottom-e.y,o=r.right-e.x,s=nx(this._overlayRef.getConfig().minHeight),a=nx(this._overlayRef.getConfig().minWidth),c=t.fitsInViewportVertically||s!=null&&s<=i,l=t.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(t,e,r){if(this._previousPushAmount&&this._positionLocked)return{x:t.x+this._previousPushAmount.x,y:t.y+this._previousPushAmount.y};let i=rx(e),o=this._viewportRect,s=Math.max(t.x+i.width-o.width,0),a=Math.max(t.y+i.height-o.height,0),c=Math.max(o.top-r.top-t.y,0),l=Math.max(o.left-r.left-t.x,0),d=0,u=0;return i.width<=o.width?d=l||-s:d=t.x<this._getViewportMarginStart()?o.left-r.left-t.x:0,i.height<=o.height?u=c||-a:u=t.y<this._getViewportMarginTop()?o.top-r.top-t.y:0,this._previousPushAmount={x:d,y:u},{x:t.x+d,y:t.y+u}}_applyPosition(t,e){if(this._setTransformOrigin(t),this._setOverlayElementStyles(e,t),this._setBoundingBoxStyles(e,t),t.panelClass&&this._addPanelClasses(t.panelClass),this._positionChanges.observers.length){let r=this._getScrollVisibility();if(t!==this._lastPosition||!this._lastScrollVisibility||!c1(this._lastScrollVisibility,r)){let i=new nd(t,r);this._positionChanges.next(i)}this._lastScrollVisibility=r}this._lastPosition=t,this._isInitialRender=!1}_setTransformOrigin(t){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),r,i=t.overlayY;t.overlayX==="center"?r="center":this._isRtl()?r=t.overlayX==="start"?"right":"left":r=t.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${r} ${i}`}_calculateBoundingBoxRect(t,e){let r=this._viewportRect,i=this._isRtl(),o,s,a;if(e.overlayY==="top")s=t.y,o=r.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=r.height-t.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=r.height-a+this._getViewportMarginTop();else{let h=Math.min(r.bottom-t.y+r.top,t.y),p=this._lastBoundingBoxSize.height;o=h*2,s=t.y-h,o>p&&!this._isInitialRender&&!this._growAfterOpen&&(s=t.y-p/2)}let c=e.overlayX==="start"&&!i||e.overlayX==="end"&&i,l=e.overlayX==="end"&&!i||e.overlayX==="start"&&i,d,u,m;if(l)m=r.width-t.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=t.x-this._getViewportMarginStart();else if(c)u=t.x,d=r.right-t.x-this._getViewportMarginEnd();else{let h=Math.min(r.right-t.x+r.left,t.x),p=this._lastBoundingBoxSize.width;d=h*2,u=t.x-h,d>p&&!this._isInitialRender&&!this._growAfterOpen&&(u=t.x-p/2)}return{top:s,left:u,bottom:a,right:m,width:d,height:o}}_setBoundingBoxStyles(t,e){let r=this._calculateBoundingBoxRect(t,e);!this._isInitialRender&&!this._growAfterOpen&&(r.height=Math.min(r.height,this._lastBoundingBoxSize.height),r.width=Math.min(r.width,this._lastBoundingBoxSize.width));let i={};if(this._hasExactPosition())i.top=i.left="0",i.bottom=i.right="auto",i.maxHeight=i.maxWidth="",i.width=i.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;i.width=Qe(r.width),i.height=Qe(r.height),i.top=Qe(r.top)||"auto",i.bottom=Qe(r.bottom)||"auto",i.left=Qe(r.left)||"auto",i.right=Qe(r.right)||"auto",e.overlayX==="center"?i.alignItems="center":i.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?i.justifyContent="center":i.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(i.maxHeight=Qe(o)),s&&(i.maxWidth=Qe(s))}this._lastBoundingBoxSize=r,Bi(this._boundingBox.style,i)}_resetBoundingBoxStyles(){Bi(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){Bi(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(t,e){let r={},i=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(i){let d=this._viewportRuler.getViewportScrollPosition();Bi(r,this._getExactOverlayY(e,t,d)),Bi(r,this._getExactOverlayX(e,t,d))}else r.position="static";let a="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),r.transform=a.trim(),s.maxHeight&&(i?r.maxHeight=Qe(s.maxHeight):o&&(r.maxHeight="")),s.maxWidth&&(i?r.maxWidth=Qe(s.maxWidth):o&&(r.maxWidth="")),Bi(this._pane.style,r)}_getExactOverlayY(t,e,r){let i={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,t);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,r)),t.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;i.bottom=`${s-(o.y+this._overlayRect.height)}px`}else i.top=Qe(o.y);return i}_getExactOverlayX(t,e,r){let i={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,t);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,r));let s;if(this._isRtl()?s=t.overlayX==="end"?"left":"right":s=t.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;i.right=`${a-(o.x+this._overlayRect.width)}px`}else i.left=Qe(o.x);return i}_getScrollVisibility(){let t=this._getOriginRect(),e=this._pane.getBoundingClientRect(),r=this._scrollables.map(i=>i.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:J0(t,r),isOriginOutsideView:rp(t,r),isOverlayClipped:J0(e,r),isOverlayOutsideView:rp(e,r)}}_subtractOverflows(t,...e){return e.reduce((r,i)=>r-Math.max(i,0),t)}_getNarrowedViewportRect(){let t=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,r=this._viewportRuler.getViewportScrollPosition();return{top:r.top+this._getViewportMarginTop(),left:r.left+this._getViewportMarginStart(),right:r.left+t-this._getViewportMarginEnd(),bottom:r.top+e-this._getViewportMarginBottom(),width:t-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(t,e){return e==="x"?t.offsetX==null?this._offsetX:t.offsetX:t.offsetY==null?this._offsetY:t.offsetY}_validatePositions(){}_addPanelClasses(t){this._pane&&Oo(t).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(t=>{this._pane.classList.remove(t)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let t=this._origin;if(t instanceof z)return t.nativeElement.getBoundingClientRect();if(t instanceof Element)return t.getBoundingClientRect();let e=t.width||0,r=t.height||0;return{top:t.y,bottom:t.y+r,left:t.x,right:t.x+e,height:r,width:e}}_getContainerRect(){let t=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();t&&(e.style.display="block");let r=e.getBoundingClientRect();return t&&(e.style.display=""),r}};function Bi(n,t){for(let e in t)t.hasOwnProperty(e)&&(n[e]=t[e]);return n}function nx(n){if(typeof n!="number"&&n!=null){let[t,e]=n.split(a1);return!e||e==="px"?parseFloat(t):null}return n||null}function rx(n){return{top:Math.floor(n.top),right:Math.floor(n.right),bottom:Math.floor(n.bottom),left:Math.floor(n.left),width:Math.floor(n.width),height:Math.floor(n.height)}}function c1(n,t){return n===t?!0:n.isOriginClipped===t.isOriginClipped&&n.isOriginOutsideView===t.isOriginOutsideView&&n.isOverlayClipped===t.isOverlayClipped&&n.isOverlayOutsideView===t.isOverlayOutsideView}var ix="cdk-global-overlay-wrapper";function Vi(n){return new id}var id=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(t){let e=t.getConfig();this._overlayRef=t,this._width&&!e.width&&t.updateSize({width:this._width}),this._height&&!e.height&&t.updateSize({height:this._height}),t.hostElement.classList.add(ix),this._isDisposed=!1}top(t=""){return this._bottomOffset="",this._topOffset=t,this._alignItems="flex-start",this}left(t=""){return this._xOffset=t,this._xPosition="left",this}bottom(t=""){return this._topOffset="",this._bottomOffset=t,this._alignItems="flex-end",this}right(t=""){return this._xOffset=t,this._xPosition="right",this}start(t=""){return this._xOffset=t,this._xPosition="start",this}end(t=""){return this._xOffset=t,this._xPosition="end",this}width(t=""){return this._overlayRef?this._overlayRef.updateSize({width:t}):this._width=t,this}height(t=""){return this._overlayRef?this._overlayRef.updateSize({height:t}):this._height=t,this}centerHorizontally(t=""){return this.left(t),this._xPosition="center",this}centerVertically(t=""){return this.top(t),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let t=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,r=this._overlayRef.getConfig(),{width:i,height:o,maxWidth:s,maxHeight:a}=r,c=(i==="100%"||i==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,u=this._xOffset,m=this._overlayRef.getConfig().direction==="rtl",h="",p="",g="";c?g="flex-start":d==="center"?(g="center",m?p=u:h=u):m?d==="left"||d==="end"?(g="flex-end",h=u):(d==="right"||d==="start")&&(g="flex-start",p=u):d==="left"||d==="start"?(g="flex-start",h=u):(d==="right"||d==="end")&&(g="flex-end",p=u),t.position=this._cssPosition,t.marginLeft=c?"0":h,t.marginTop=l?"0":this._topOffset,t.marginBottom=this._bottomOffset,t.marginRight=c?"0":p,e.justifyContent=g,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let t=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,r=e.style;e.classList.remove(ix),r.justifyContent=r.alignItems=t.marginTop=t.marginBottom=t.marginLeft=t.marginRight=t.position="",this._overlayRef=null,this._isDisposed=!0}};var oa=new x("OVERLAY_DEFAULT_CONFIG");function Bo(n,t){n.get(vt).load(cx);let e=n.get(ad),r=n.get(B),i=n.get(Fe),o=n.get(Qt),s=n.get(wn),a=n.get(lt,null,{optional:!0})||n.get(ct).createRenderer(null,null),c=new ur(t),l=n.get(oa,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in r.body?c.usePopover=t?.usePopover??l:c.usePopover=!1;let d=r.createElement("div"),u=r.createElement("div");d.id=i.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),u.appendChild(d),c.usePopover&&(u.setAttribute("popover","manual"),u.classList.add("cdk-overlay-popover"));let m=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return op(m)?m.after(u):m?.type==="parent"?m.element.appendChild(u):e.getContainerElement().appendChild(u),new Fo(new Xl(d,o,n),u,d,c,n.get(F),n.get(sx),r,n.get(Ol),n.get(ax),t?.disableAnimations??n.get(As,null,{optional:!0})==="NoopAnimations",n.get(qe),a)}var l1=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],d1=new x("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let n=f(V);return()=>sd(n)}}),Lo=(()=>{class n{elementRef=f(z);constructor(){}static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return n})(),dx=new x("cdk-connected-overlay-default-config"),cd=(()=>{class n{_dir=f(wn,{optional:!0});_injector=f(V);_overlayRef;_templatePortal;_backdropSubscription=ye.EMPTY;_attachSubscription=ye.EMPTY;_detachSubscription=ye.EMPTY;_positionSubscription=ye.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=f(d1);_ngZone=f(F);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new j;positionChange=new j;attach=new j;detach=new j;overlayKeydown=new j;overlayOutsideClick=new j;constructor(){let e=f(yt),r=f(Lt),i=f(dx,{optional:!0}),o=f(oa,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Cn(e,r),this.scrollStrategy=this._scrollStrategyFactory(),i&&this._assignConfig(i)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=l1);let e=this._overlayRef=Bo(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(r=>{this.overlayKeydown.next(r),r.keyCode===27&&!this.disableClose&&!xt(r)&&(r.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(r=>{let i=this._getOriginElement(),o=mt(r);(!i||i!==o&&!i.contains(o))&&this.overlayOutsideClick.next(r)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),r=new ur({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(r.height=this.height),(this.minWidth||this.minWidth===0)&&(r.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(r.minHeight=this.minHeight),this.backdropClass&&(r.backdropClass=this.backdropClass),this.panelClass&&(r.panelClass=this.panelClass),r}_updatePositionStrategy(e){let r=this.positions.map(i=>({originX:i.originX,originY:i.originY,overlayX:i.overlayX,overlayY:i.overlayY,offsetX:i.offsetX||this.offsetX,offsetY:i.offsetY||this.offsetY,panelClass:i.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(r).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=lx(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof Lo?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof Lo?this.origin.elementRef.nativeElement:this.origin instanceof z?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(r=>this.backdropClick.emit(r)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(vu(()=>this.positionChange.observers.length>0)).subscribe(r=>{this._ngZone.run(()=>this.positionChange.emit(r)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",re],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",re],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",re],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",re],push:[2,"cdkConnectedOverlayPush","push",re],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",re],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",re],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[it]})}return n})();var nn=(function(n){return n[n.FADING_IN=0]="FADING_IN",n[n.VISIBLE=1]="VISIBLE",n[n.FADING_OUT=2]="FADING_OUT",n[n.HIDDEN=3]="HIDDEN",n})(nn||{}),sp=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=nn.HIDDEN;constructor(t,e,r,i=!1){this._renderer=t,this.element=e,this.config=r,this._animationForciblyDisabledThroughCss=i}fadeOut(){this._renderer.fadeOutRipple(this)}},ux=Ro({passive:!0,capture:!0}),ap=class{_events=new Map;addHandler(t,e,r,i){let o=this._events.get(e);if(o){let s=o.get(r);s?s.add(i):o.set(r,new Set([i]))}else this._events.set(e,new Map([[r,new Set([i])]])),t.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,ux)})}removeHandler(t,e,r){let i=this._events.get(t);if(!i)return;let o=i.get(e);o&&(o.delete(r),o.size===0&&i.delete(e),i.size===0&&(this._events.delete(t),document.removeEventListener(t,this._delegateEventHandler,ux)))}_delegateEventHandler=t=>{let e=mt(t);e&&this._events.get(t.type)?.forEach((r,i)=>{(i===e||i.contains(e))&&r.forEach(o=>o.handleEvent(t))})}},sa={enterDuration:225,exitDuration:150},u1=800,fx=Ro({passive:!0,capture:!0}),mx=["mousedown","touchstart"],hx=["mouseup","mouseleave","touchend","touchcancel"],f1=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(r,i){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return n})(),aa=class n{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new ap;constructor(t,e,r,i,o){this._target=t,this._ngZone=e,this._platform=i,i.isBrowser&&(this._containerElement=zn(r)),o&&o.get(vt).load(f1)}fadeInRipple(t,e,r={}){let i=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=w(w({},sa),r.animation);r.centered&&(t=i.left+i.width/2,e=i.top+i.height/2);let s=r.radius||m1(t,e,i),a=t-i.left,c=e-i.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,r.color!=null&&(d.style.backgroundColor=r.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),m=u.transitionProperty,h=u.transitionDuration,p=m==="none"||h==="0s"||h==="0s, 0s"||i.width===0&&i.height===0,g=new sp(this,d,r,p);d.style.transform="scale3d(1, 1, 1)",g.state=nn.FADING_IN,r.persistent||(this._mostRecentTransientRipple=g);let y=null;return!p&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let D=()=>{y&&(y.fallbackTimer=null),clearTimeout(oe),this._finishRippleTransition(g)},R=()=>this._destroyRipple(g),oe=setTimeout(R,l+100);d.addEventListener("transitionend",D),d.addEventListener("transitioncancel",R),y={onTransitionEnd:D,onTransitionCancel:R,fallbackTimer:oe}}),this._activeRipples.set(g,y),(p||!l)&&this._finishRippleTransition(g),g}fadeOutRipple(t){if(t.state===nn.FADING_OUT||t.state===nn.HIDDEN)return;let e=t.element,r=w(w({},sa),t.config.animation);e.style.transitionDuration=`${r.exitDuration}ms`,e.style.opacity="0",t.state=nn.FADING_OUT,(t._animationForciblyDisabledThroughCss||!r.exitDuration)&&this._finishRippleTransition(t)}fadeOutAll(){this._getActiveRipples().forEach(t=>t.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(t=>{t.config.persistent||t.fadeOut()})}setupTriggerEvents(t){let e=zn(t);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,mx.forEach(r=>{n._eventManager.addHandler(this._ngZone,r,e,this)}))}handleEvent(t){t.type==="mousedown"?this._onMousedown(t):t.type==="touchstart"?this._onTouchStart(t):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{hx.forEach(e=>{this._triggerElement.addEventListener(e,this,fx)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(t){t.state===nn.FADING_IN?this._startFadeOutTransition(t):t.state===nn.FADING_OUT&&this._destroyRipple(t)}_startFadeOutTransition(t){let e=t===this._mostRecentTransientRipple,{persistent:r}=t.config;t.state=nn.VISIBLE,!r&&(!e||!this._isPointerDown)&&t.fadeOut()}_destroyRipple(t){let e=this._activeRipples.get(t)??null;this._activeRipples.delete(t),this._activeRipples.size||(this._containerRect=null),t===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),t.state=nn.HIDDEN,e!==null&&(t.element.removeEventListener("transitionend",e.onTransitionEnd),t.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),t.element.remove()}_onMousedown(t){let e=Ws(t),r=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+u1;!this._target.rippleDisabled&&!e&&!r&&(this._isPointerDown=!0,this.fadeInRipple(t.clientX,t.clientY,this._target.rippleConfig))}_onTouchStart(t){if(!this._target.rippleDisabled&&!qs(t)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=t.changedTouches;if(e)for(let r=0;r<e.length;r++)this.fadeInRipple(e[r].clientX,e[r].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(t=>{let e=t.state===nn.VISIBLE||t.config.terminateOnPointerUp&&t.state===nn.FADING_IN;!t.config.persistent&&e&&t.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let t=this._triggerElement;t&&(mx.forEach(e=>n._eventManager.removeHandler(e,t,this)),this._pointerUpEventsRegistered&&(hx.forEach(e=>t.removeEventListener(e,this,fx)),this._pointerUpEventsRegistered=!1))}};function m1(n,t,e){let r=Math.max(Math.abs(n-e.left),Math.abs(n-e.right)),i=Math.max(Math.abs(t-e.top),Math.abs(t-e.bottom));return Math.sqrt(r*r+i*i)}var cp=new x("mat-ripple-global-options"),dd=(()=>{class n{_elementRef=f(z);_animationsDisabled=Ge();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=f(F),r=f(Te),i=f(cp,{optional:!0}),o=f(V);this._globalOptions=i||{},this._rippleRenderer=new aa(this,e,this._elementRef,r,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:w(w(w({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,r=0,i){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,r,w(w({},this.rippleConfig),i)):this._rippleRenderer.fadeInRipple(0,0,w(w({},this.rippleConfig),e))}static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(r,i){r&2&&Q("mat-ripple-unbounded",i.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return n})();var h1={capture:!0},p1=["focus","mousedown","mouseenter","touchstart"],lp="mat-ripple-loader-uninitialized",dp="mat-ripple-loader-class-name",px="mat-ripple-loader-centered",ud="mat-ripple-loader-disabled",gx=(()=>{class n{_document=f(B);_animationsDisabled=Ge();_globalRippleOptions=f(cp,{optional:!0});_platform=f(Te);_ngZone=f(F);_injector=f(V);_eventCleanups;_hosts=new Map;constructor(){let e=f(ct).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>p1.map(r=>e.listen(this._document,r,this._onInteraction,h1)))}ngOnDestroy(){let e=this._hosts.keys();for(let r of e)this.destroyRipple(r);this._eventCleanups.forEach(r=>r())}configureRipple(e,r){e.setAttribute(lp,this._globalRippleOptions?.namespace??""),(r.className||!e.hasAttribute(dp))&&e.setAttribute(dp,r.className||""),r.centered&&e.setAttribute(px,""),r.disabled&&e.setAttribute(ud,"")}setDisabled(e,r){let i=this._hosts.get(e);i?(i.target.rippleDisabled=r,!r&&!i.hasSetUpEvents&&(i.hasSetUpEvents=!0,i.renderer.setupTriggerEvents(e))):r?e.setAttribute(ud,""):e.removeAttribute(ud)}_onInteraction=e=>{let r=mt(e);if(r instanceof HTMLElement){let i=r.closest(`[${lp}="${this._globalRippleOptions?.namespace??""}"]`);i&&this._createRipple(i)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let r=this._document.createElement("span");r.classList.add("mat-ripple",e.getAttribute(dp)),e.append(r);let i=this._globalRippleOptions,o=this._animationsDisabled?0:i?.animation?.enterDuration??sa.enterDuration,s=this._animationsDisabled?0:i?.animation?.exitDuration??sa.exitDuration,a={rippleDisabled:this._animationsDisabled||i?.disabled||e.hasAttribute(ud),rippleConfig:{centered:e.hasAttribute(px),terminateOnPointerUp:i?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new aa(a,this._ngZone,r,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(lp)}destroyRipple(e){let r=this._hosts.get(e);r&&(r.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Vr=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["structural-styles"]],decls:0,vars:0,template:function(r,i){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return n})();var g1=new x("MAT_BUTTON_CONFIG");function bx(n){return n==null?void 0:jt(n)}var yx=(()=>{class n{_elementRef=f(z);_ngZone=f(F);_animationsDisabled=Ge();_config=f(g1,{optional:!0});_focusMonitor=f(Pi);_cleanupClick;_renderer=f(lt);_rippleLoader=f(gx);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){f(vt).load(Vr);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",r){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,r):this._elementRef.nativeElement.focus(r)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(r,i){r&2&&(ue("disabled",i._getDisabledAttribute())("aria-disabled",i._getAriaDisabled())("tabindex",i._getTabIndex()),vn(i.color?"mat-"+i.color:""),Q("mat-mdc-button-disabled",i.disabled)("mat-mdc-button-disabled-interactive",i.disabledInteractive)("mat-unthemed",!i.color)("_mat-animation-noopable",i._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",re],disabled:[2,"disabled","disabled",re],ariaDisabled:[2,"aria-disabled","ariaDisabled",re],disabledInteractive:[2,"disabledInteractive","disabledInteractive",re],tabIndex:[2,"tabIndex","tabIndex",bx],_tabindex:[2,"tabindex","_tabindex",bx]}})}return n})();var b1=["matButton",""],y1=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],_1=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var _x=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),vx=(()=>{class n extends yx{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=v1(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let r=this._elementRef.nativeElement.classList,i=this._appearance?_x.get(this._appearance):null,o=_x.get(e);i&&r.remove(...i),r.add(...o),this._appearance=e}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Pe],attrs:b1,ngContentSelectors:_1,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(r,i){r&1&&(Ze(y1),Jt(0,"span",0),ae(1),Bt(2,"span",1),ae(3,1),Xt(),ae(4,2),Jt(5,"span",2)(6,"span",3)),r&2&&Q("mdc-button__ripple",!i._isFab)("mdc-fab__ripple",i._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return n})();function v1(n){return n.hasAttribute("mat-raised-button")?"elevated":n.hasAttribute("mat-stroked-button")?"outlined":n.hasAttribute("mat-flat-button")?"filled":n.hasAttribute("mat-button")?"text":null}function x1(n,t){if(n&1){let e=jn();I(0,"div",1)(1,"button",2),Ce("click",function(){Yt(e);let i=ne();return Zt(i.action())}),_t(2),T()()}if(n&2){let e=ne();S(2),en(" ",e.data.action," ")}}var D1=["label"];function w1(n,t){}var C1=Math.pow(2,31)-1,ca=class{_overlayRef;instance;containerInstance;_afterDismissed=new E;_afterOpened=new E;_onAction=new E;_durationTimeoutId;_dismissedByAction=!1;constructor(t,e){this._overlayRef=e,this.containerInstance=t,t._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(t){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(t,C1))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},xx=new x("MatSnackBarData"),Vo=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},E1=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return n})(),k1=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return n})(),I1=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return n})(),S1=(()=>{class n{snackBarRef=f(ca);data=f(xx);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(r,i){r&1&&(I(0,"div",0),_t(1),T(),pe(2,x1,3,1,"div",1)),r&2&&(S(),en(" ",i.data.message,`
`),S(),ge(i.hasAction?2:-1))},dependencies:[vx,E1,k1,I1],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return n})(),up="_mat-snack-bar-enter",fp="_mat-snack-bar-exit",T1=(()=>{class n extends Br{_ngZone=f(F);_elementRef=f(z);_changeDetectorRef=f(tt);_platform=f(Te);_animationsDisabled=Ge();snackBarConfig=f(Vo);_document=f(B);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=f(V);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new E;_onExit=new E;_onEnter=new E;_animationState="void";_live;_label;_role;_liveElementId=f(Fe).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let r=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),r}attachTemplatePortal(e){this._assertNotAttached();let r=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),r}attachDomPortal=e=>{this._assertNotAttached();let r=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),r};onAnimationEnd(e){e===fp?this._completeExit():e===up&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?Ft(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(up)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(up)},200)))}exit(){return this._destroyed?An(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?Ft(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(fp)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(fp),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,r=this.snackBarConfig.panelClass;r&&(Array.isArray(r)?r.forEach(s=>e.classList.add(s)):e.classList.add(r)),this._exposeToModals();let i=this._label.nativeElement,o="mdc-snackbar__label";i.classList.toggle(o,!i.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,r=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let i=0;i<r.length;i++){let o=r[i],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let r=e.getAttribute("aria-owns");if(r){let i=r.replace(this._liveElementId,"").trim();i.length>0?e.setAttribute("aria-owns",i):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,r=e.querySelector("[aria-hidden]"),i=e.querySelector("[aria-live]");if(r&&i){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&r.contains(document.activeElement)&&(o=document.activeElement),r.removeAttribute("aria-hidden"),i.appendChild(r),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-snack-bar-container"]],viewQuery:function(r,i){if(r&1&&et(Un,7)(D1,7),r&2){let o;J(o=ee())&&(i._portalOutlet=o.first),J(o=ee())&&(i._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(r,i){r&1&&Ce("animationend",function(s){return i.onAnimationEnd(s.animationName)})("animationcancel",function(s){return i.onAnimationEnd(s.animationName)}),r&2&&Q("mat-snack-bar-container-enter",i._animationState==="visible")("mat-snack-bar-container-exit",i._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!i._animationsDisabled)},features:[Pe],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(r,i){r&1&&(I(0,"div",1)(1,"div",2,0)(3,"div",3),Ue(4,w1,0,0,"ng-template",4),T(),fe(5,"div"),T()()),r&2&&(S(5),ue("aria-live",i._live)("role",i._role)("id",i._liveElementId))},dependencies:[Un],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--mat-snack-bar-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-snack-bar-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-snack-bar-container-color, var(--mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--mat-snack-bar-supporting-text-font, var(--mat-sys-body-medium-font));
  font-size: var(--mat-snack-bar-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-snack-bar-supporting-text-weight, var(--mat-sys-body-medium-weight));
  line-height: var(--mat-snack-bar-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--mat-snack-bar-button-color, var(--mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --mat-button-text-state-layer-color: currentColor;
  --mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2})}return n})(),mp=new x("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Vo}),Dx=(()=>{class n{_live=f(Xs);_injector=f(V);_breakpointObserver=f(zh);_parentSnackBar=f(n,{optional:!0,skipSelf:!0});_defaultConfig=f(mp);_animationsDisabled=Ge();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=S1;snackBarContainerComponent=T1;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,r){return this._attach(e,r)}openFromTemplate(e,r){return this._attach(e,r)}open(e,r="",i){let o=w(w({},this._defaultConfig),i);return o.data={message:e,action:r},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,r){let i=r&&r.viewContainerRef&&r.viewContainerRef.injector,o=V.create({parent:i||this._injector,providers:[{provide:Vo,useValue:r}]}),s=new dr(this.snackBarContainerComponent,r.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=r,a.instance}_attach(e,r){let i=w(w(w({},new Vo),this._defaultConfig),r),o=this._createOverlay(i),s=this._attachSnackBarContainer(o,i),a=new ca(s,o);if(e instanceof yt){let c=new Cn(e,null,{$implicit:i.data,snackBarRef:a});a.instance=s.attachTemplatePortal(c)}else{let c=this._createInjector(i,a),l=new dr(e,void 0,c),d=s.attachComponentPortal(l);a.instance=d.instance}return this._breakpointObserver.observe(P0.HandsetPortrait).pipe(st(o.detachments())).subscribe(c=>{o.overlayElement.classList.toggle(this.handsetCssClass,c.matches)}),i.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(i.announcementMessage,i.politeness)}),this._animateSnackBar(a,i),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,r){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),r.announcementMessage&&this._live.clear()}),r.duration&&r.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(r.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let r=new ur;r.direction=e.direction;let i=Vi(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?i.left("0"):a?i.right("0"):i.centerHorizontally(),e.verticalPosition==="top"?i.top("0"):i.bottom("0"),r.positionStrategy=i,r.disableAnimations=this._animationsDisabled,Bo(this._injector,r)}_createInjector(e,r){let i=e&&e.viewContainerRef&&e.viewContainerRef.injector;return V.create({parent:i||this._injector,providers:[{provide:ca,useValue:r},{provide:xx,useValue:e.data}]})}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var wx={providers:[kf(),{provide:Ql,useValue:{appearance:"outline",subscriptSizing:"dynamic"}},{provide:mp,useValue:{}}]};var M1=["*",[["mat-toolbar-row"]]],A1=["*","mat-toolbar-row"],R1=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return n})(),Cx=(()=>{class n{_elementRef=f(z);_platform=f(Te);_document=f(B);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-toolbar"]],contentQueries:function(r,i,o){if(r&1&&_n(o,R1,5),r&2){let s;J(s=ee())&&(i._toolbarRows=s)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(r,i){r&2&&(vn(i.color?"mat-"+i.color:""),Q("mat-toolbar-multiple-rows",i._toolbarRows.length>0)("mat-toolbar-single-row",i._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:A1,decls:2,vars:0,template:function(r,i){r&1&&(Ze(M1),ae(0),ae(1,1))},styles:[`.mat-toolbar {
  background: var(--mat-toolbar-container-background-color, var(--mat-sys-surface));
  color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}
.mat-toolbar, .mat-toolbar h1, .mat-toolbar h2, .mat-toolbar h3, .mat-toolbar h4, .mat-toolbar h5, .mat-toolbar h6 {
  font-family: var(--mat-toolbar-title-text-font, var(--mat-sys-title-large-font));
  font-size: var(--mat-toolbar-title-text-size, var(--mat-sys-title-large-size));
  line-height: var(--mat-toolbar-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-weight: var(--mat-toolbar-title-text-weight, var(--mat-sys-title-large-weight));
  letter-spacing: var(--mat-toolbar-title-text-tracking, var(--mat-sys-title-large-tracking));
  margin: 0;
}
@media (forced-colors: active) {
  .mat-toolbar {
    outline: solid 1px;
  }
}
.mat-toolbar .mat-form-field-underline,
.mat-toolbar .mat-form-field-ripple,
.mat-toolbar .mat-focused .mat-form-field-ripple {
  background-color: currentColor;
}
.mat-toolbar .mat-form-field-label,
.mat-toolbar .mat-focused .mat-form-field-label,
.mat-toolbar .mat-select-value,
.mat-toolbar .mat-select-arrow,
.mat-toolbar .mat-form-field.mat-focused .mat-select-arrow {
  color: inherit;
}
.mat-toolbar .mat-input-element {
  caret-color: currentColor;
}
.mat-toolbar .mat-mdc-button-base.mat-mdc-button-base.mat-unthemed {
  --mat-button-text-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
  --mat-button-outlined-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}

.mat-toolbar-row, .mat-toolbar-single-row {
  display: flex;
  box-sizing: border-box;
  padding: 0 16px;
  width: 100%;
  flex-direction: row;
  align-items: center;
  white-space: nowrap;
  height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-row, .mat-toolbar-single-row {
    height: var(--mat-toolbar-mobile-height, 56px);
  }
}

.mat-toolbar-multiple-rows {
  display: flex;
  box-sizing: border-box;
  flex-direction: column;
  width: 100%;
  min-height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-multiple-rows {
    min-height: var(--mat-toolbar-mobile-height, 56px);
  }
}
`],encapsulation:2,changeDetection:0})}return n})();var la=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new E;constructor(t=!1,e,r=!0,i){this._multiple=t,this._emitChanges=r,this.compareWith=i,e&&e.length&&(t?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...t){this._verifyValueAssignment(t),t.forEach(r=>this._markSelected(r));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...t){this._verifyValueAssignment(t),t.forEach(r=>this._unmarkSelected(r));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...t){this._verifyValueAssignment(t);let e=this.selected,r=new Set(t.map(o=>this._getConcreteValue(o)));t.forEach(o=>this._markSelected(o)),e.filter(o=>!r.has(this._getConcreteValue(o,r))).forEach(o=>this._unmarkSelected(o));let i=this._hasQueuedChanges();return this._emitChangeEvent(),i}toggle(t){return this.isSelected(t)?this.deselect(t):this.select(t)}clear(t=!0){this._unmarkAll();let e=this._hasQueuedChanges();return t&&this._emitChangeEvent(),e}isSelected(t){return this._selection.has(this._getConcreteValue(t))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(t){this._multiple&&this.selected&&this._selected.sort(t)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(t){t=this._getConcreteValue(t),this.isSelected(t)||(this._multiple||this._unmarkAll(),this.isSelected(t)||this._selection.add(t),this._emitChanges&&this._selectedToEmit.push(t))}_unmarkSelected(t){t=this._getConcreteValue(t),this.isSelected(t)&&(this._selection.delete(t),this._emitChanges&&this._deselectedToEmit.push(t))}_unmarkAll(){this.isEmpty()||this._selection.forEach(t=>this._unmarkSelected(t))}_verifyValueAssignment(t){t.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(t,e){if(this.compareWith){e=e??this._selection;for(let r of e)if(this.compareWith(t,r))return r;return t}else return t}};var da=(()=>{class n{_listeners=[];notify(e,r){for(let i of this._listeners)i(e,r)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(r=>e!==r)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Rx=(()=>{class n{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,r){this._renderer=e,this._elementRef=r}setProperty(e,r){this._renderer.setProperty(this._elementRef.nativeElement,e,r)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(r){return new(r||n)(Ne(lt),Ne(z))};static \u0275dir=Y({type:n})}return n})(),O1=(()=>{class n extends Rx{static \u0275fac=(()=>{let e;return function(i){return(e||(e=Kt(n)))(i||n)}})();static \u0275dir=Y({type:n,features:[Pe]})}return n})(),Cd=new x("");var N1={provide:Cd,useExisting:Ut(()=>Ox),multi:!0};function P1(){let n=tn()?tn().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var F1=new x(""),Ox=(()=>{class n extends Rx{_compositionMode;_composing=!1;constructor(e,r,i){super(e,r),this._compositionMode=i,this._compositionMode==null&&(this._compositionMode=!P1())}writeValue(e){let r=e??"";this.setProperty("value",r)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(r){return new(r||n)(Ne(lt),Ne(z),Ne(F1,8))};static \u0275dir=Y({type:n,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(r,i){r&1&&Ce("input",function(s){return i._handleInput(s.target.value)})("blur",function(){return i.onTouched()})("compositionstart",function(){return i._compositionStart()})("compositionend",function(s){return i._compositionEnd(s.target.value)})},standalone:!1,features:[Ke([N1]),Pe]})}return n})();function gp(n){return n==null||bp(n)===0}function bp(n){return n==null?null:Array.isArray(n)||typeof n=="string"?n.length:n instanceof Set?n.size:null}var ga=new x(""),yp=new x(""),L1=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,md=class{static min(t){return B1(t)}static max(t){return V1(t)}static required(t){return j1(t)}static requiredTrue(t){return H1(t)}static email(t){return z1(t)}static minLength(t){return U1(t)}static maxLength(t){return $1(t)}static pattern(t){return G1(t)}static nullValidator(t){return Nx()}static compose(t){return jx(t)}static composeAsync(t){return Hx(t)}};function B1(n){return t=>{if(t.value==null||n==null)return null;let e=parseFloat(t.value);return!isNaN(e)&&e<n?{min:{min:n,actual:t.value}}:null}}function V1(n){return t=>{if(t.value==null||n==null)return null;let e=parseFloat(t.value);return!isNaN(e)&&e>n?{max:{max:n,actual:t.value}}:null}}function j1(n){return gp(n.value)?{required:!0}:null}function H1(n){return n.value===!0?null:{required:!0}}function z1(n){return gp(n.value)||L1.test(n.value)?null:{email:!0}}function U1(n){return t=>{let e=t.value?.length??bp(t.value);return e===null||e===0?null:e<n?{minlength:{requiredLength:n,actualLength:e}}:null}}function $1(n){return t=>{let e=t.value?.length??bp(t.value);return e!==null&&e>n?{maxlength:{requiredLength:n,actualLength:e}}:null}}function G1(n){if(!n)return Nx;let t,e;return typeof n=="string"?(e="",n.charAt(0)!=="^"&&(e+="^"),e+=n,n.charAt(n.length-1)!=="$"&&(e+="$"),t=new RegExp(e)):(e=n.toString(),t=n),r=>{if(gp(r.value))return null;let i=r.value;return t.test(i)?null:{pattern:{requiredPattern:e,actualValue:i}}}}function Nx(n){return null}function Px(n){return n!=null}function Fx(n){return Io(n)?zt(n):n}function Lx(n){let t={};return n.forEach(e=>{t=e!=null?w(w({},t),e):t}),Object.keys(t).length===0?null:t}function Bx(n,t){return t.map(e=>e(n))}function W1(n){return!n.validate}function Vx(n){return n.map(t=>W1(t)?t:e=>t.validate(e))}function jx(n){if(!n)return null;let t=n.filter(Px);return t.length==0?null:function(e){return Lx(Bx(e,t))}}function _p(n){return n!=null?jx(Vx(n)):null}function Hx(n){if(!n)return null;let t=n.filter(Px);return t.length==0?null:function(e){let r=Bx(e,t).map(Fx);return gu(r).pipe(Le(Lx))}}function vp(n){return n!=null?Hx(Vx(n)):null}function Ex(n,t){return n===null?[t]:Array.isArray(n)?[...n,t]:[n,t]}function zx(n){return n._rawValidators}function Ux(n){return n._rawAsyncValidators}function hp(n){return n?Array.isArray(n)?n:[n]:[]}function hd(n,t){return Array.isArray(n)?n.includes(t):n===t}function kx(n,t){let e=hp(t);return hp(n).forEach(i=>{hd(e,i)||e.push(i)}),e}function Ix(n,t){return hp(t).filter(e=>!hd(n,e))}var pd=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(t){this._rawValidators=t||[],this._composedValidatorFn=_p(this._rawValidators)}_setAsyncValidators(t){this._rawAsyncValidators=t||[],this._composedAsyncValidatorFn=vp(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(t){this._onDestroyCallbacks.push(t)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(t=>t()),this._onDestroyCallbacks=[]}reset(t=void 0){this.control?.reset(t)}hasError(t,e){return this.control?this.control.hasError(t,e):!1}getError(t,e){return this.control?this.control.getError(t,e):null}},ji=class extends pd{name;get formDirective(){return null}get path(){return null}},Hi=class extends pd{_parent=null;name=null;valueAccessor=null},pp=class{_cd;constructor(t){this._cd=t}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var $x=(()=>{class n extends pp{constructor(e){super(e)}static \u0275fac=function(r){return new(r||n)(Ne(Hi,2))};static \u0275dir=Y({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(r,i){r&2&&Q("ng-untouched",i.isUntouched)("ng-touched",i.isTouched)("ng-pristine",i.isPristine)("ng-dirty",i.isDirty)("ng-valid",i.isValid)("ng-invalid",i.isInvalid)("ng-pending",i.isPending)},standalone:!1,features:[Pe]})}return n})();var ua="VALID",fd="INVALID",jo="PENDING",fa="DISABLED",jr=class{},gd=class extends jr{value;source;constructor(t,e){super(),this.value=t,this.source=e}},ha=class extends jr{pristine;source;constructor(t,e){super(),this.pristine=t,this.source=e}},pa=class extends jr{touched;source;constructor(t,e){super(),this.touched=t,this.source=e}},Ho=class extends jr{status;source;constructor(t,e){super(),this.status=t,this.source=e}},bd=class extends jr{source;constructor(t){super(),this.source=t}},yd=class extends jr{source;constructor(t){super(),this.source=t}};function Gx(n){return(Ed(n)?n.validators:n)||null}function q1(n){return Array.isArray(n)?_p(n):n||null}function Wx(n,t){return(Ed(t)?t.asyncValidators:n)||null}function Y1(n){return Array.isArray(n)?vp(n):n||null}function Ed(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}function Z1(n,t,e){let r=n.controls;if(!(t?Object.keys(r):r).length)throw new M(1e3,"");if(!r[e])throw new M(1001,"")}function K1(n,t,e){n._forEachChild((r,i)=>{if(e[i]===void 0)throw new M(-1002,"")})}var _d=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(t,e){this._assignValidators(t),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(t){this._rawValidators=this._composedValidatorFn=t}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(t){this._rawAsyncValidators=this._composedAsyncValidatorFn=t}get parent(){return this._parent}get status(){return $e(this.statusReactive)}set status(t){$e(()=>this.statusReactive.set(t))}_status=Me(()=>this.statusReactive());statusReactive=X(void 0);get valid(){return this.status===ua}get invalid(){return this.status===fd}get pending(){return this.status===jo}get disabled(){return this.status===fa}get enabled(){return this.status!==fa}errors;get pristine(){return $e(this.pristineReactive)}set pristine(t){$e(()=>this.pristineReactive.set(t))}_pristine=Me(()=>this.pristineReactive());pristineReactive=X(!0);get dirty(){return!this.pristine}get touched(){return $e(this.touchedReactive)}set touched(t){$e(()=>this.touchedReactive.set(t))}_touched=Me(()=>this.touchedReactive());touchedReactive=X(!1);get untouched(){return!this.touched}_events=new E;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(t){this._assignValidators(t)}setAsyncValidators(t){this._assignAsyncValidators(t)}addValidators(t){this.setValidators(kx(t,this._rawValidators))}addAsyncValidators(t){this.setAsyncValidators(kx(t,this._rawAsyncValidators))}removeValidators(t){this.setValidators(Ix(t,this._rawValidators))}removeAsyncValidators(t){this.setAsyncValidators(Ix(t,this._rawAsyncValidators))}hasValidator(t){return hd(this._rawValidators,t)}hasAsyncValidator(t){return hd(this._rawAsyncValidators,t)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(t={}){let e=this.touched===!1;this.touched=!0;let r=t.sourceControl??this;t.onlySelf||this._parent?.markAsTouched(W(w({},t),{sourceControl:r})),e&&t.emitEvent!==!1&&this._events.next(new pa(!0,r))}markAllAsDirty(t={}){this.markAsDirty({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(t))}markAllAsTouched(t={}){this.markAsTouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(t))}markAsUntouched(t={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let r=t.sourceControl??this;this._forEachChild(i=>{i.markAsUntouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:r})}),t.onlySelf||this._parent?._updateTouched(t,r),e&&t.emitEvent!==!1&&this._events.next(new pa(!1,r))}markAsDirty(t={}){let e=this.pristine===!0;this.pristine=!1;let r=t.sourceControl??this;t.onlySelf||this._parent?.markAsDirty(W(w({},t),{sourceControl:r})),e&&t.emitEvent!==!1&&this._events.next(new ha(!1,r))}markAsPristine(t={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let r=t.sourceControl??this;this._forEachChild(i=>{i.markAsPristine({onlySelf:!0,emitEvent:t.emitEvent})}),t.onlySelf||this._parent?._updatePristine(t,r),e&&t.emitEvent!==!1&&this._events.next(new ha(!0,r))}markAsPending(t={}){this.status=jo;let e=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new Ho(this.status,e)),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.markAsPending(W(w({},t),{sourceControl:e}))}disable(t={}){let e=this._parentMarkedDirty(t.onlySelf);this.status=fa,this.errors=null,this._forEachChild(i=>{i.disable(W(w({},t),{onlySelf:!0}))}),this._updateValue();let r=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new gd(this.value,r)),this._events.next(new Ho(this.status,r)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(W(w({},t),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!0))}enable(t={}){let e=this._parentMarkedDirty(t.onlySelf);this.status=ua,this._forEachChild(r=>{r.enable(W(w({},t),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent}),this._updateAncestors(W(w({},t),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!1))}_updateAncestors(t,e){t.onlySelf||(this._parent?.updateValueAndValidity(t),t.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(t){this._parent=t}getRawValue(){return this.value}updateValueAndValidity(t={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let r=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===ua||this.status===jo)&&this._runAsyncValidator(r,t.emitEvent)}let e=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new gd(this.value,e)),this._events.next(new Ho(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.updateValueAndValidity(W(w({},t),{sourceControl:e}))}_updateTreeValidity(t={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(t)),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?fa:ua}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(t,e){if(this.asyncValidator){this.status=jo,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:t!==!1};let r=Fx(this.asyncValidator(this));this._asyncValidationSubscription=r.subscribe(i=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(i,{emitEvent:e,shouldHaveEmitted:t})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let t=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,t}return!1}setErrors(t,e={}){this.errors=t,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(t){let e=t;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((r,i)=>r&&r._find(i),this)}getError(t,e){let r=e?this.get(e):this;return r?.errors?r.errors[t]:null}hasError(t,e){return!!this.getError(t,e)}get root(){let t=this;for(;t._parent;)t=t._parent;return t}_updateControlsErrors(t,e,r){this.status=this._calculateStatus(),t&&this.statusChanges.emit(this.status),(t||r)&&this._events.next(new Ho(this.status,e)),this._parent&&this._parent._updateControlsErrors(t,e,r)}_initObservables(){this.valueChanges=new j,this.statusChanges=new j}_calculateStatus(){return this._allControlsDisabled()?fa:this.errors?fd:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(jo)?jo:this._anyControlsHaveStatus(fd)?fd:ua}_anyControlsHaveStatus(t){return this._anyControls(e=>e.status===t)}_anyControlsDirty(){return this._anyControls(t=>t.dirty)}_anyControlsTouched(){return this._anyControls(t=>t.touched)}_updatePristine(t,e){let r=!this._anyControlsDirty(),i=this.pristine!==r;this.pristine=r,t.onlySelf||this._parent?._updatePristine(t,e),i&&this._events.next(new ha(this.pristine,e))}_updateTouched(t={},e){this.touched=this._anyControlsTouched(),this._events.next(new pa(this.touched,e)),t.onlySelf||this._parent?._updateTouched(t,e)}_onDisabledChange=[];_registerOnCollectionChange(t){this._onCollectionChange=t}_setUpdateStrategy(t){Ed(t)&&t.updateOn!=null&&(this._updateOn=t.updateOn)}_parentMarkedDirty(t){return!t&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(t){return null}_assignValidators(t){this._rawValidators=Array.isArray(t)?t.slice():t,this._composedValidatorFn=q1(this._rawValidators)}_assignAsyncValidators(t){this._rawAsyncValidators=Array.isArray(t)?t.slice():t,this._composedAsyncValidatorFn=Y1(this._rawAsyncValidators)}},vd=class extends _d{constructor(t,e,r){super(Gx(e),Wx(r,e)),this.controls=t,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(t,e){return this.controls[t]?this.controls[t]:(this.controls[t]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(t,e,r={}){this.registerControl(t,e),this.updateValueAndValidity({emitEvent:r.emitEvent}),this._onCollectionChange()}removeControl(t,e={}){this.controls[t]&&this.controls[t]._registerOnCollectionChange(()=>{}),delete this.controls[t],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(t,e,r={}){this.controls[t]&&this.controls[t]._registerOnCollectionChange(()=>{}),delete this.controls[t],e&&this.registerControl(t,e),this.updateValueAndValidity({emitEvent:r.emitEvent}),this._onCollectionChange()}contains(t){return this.controls.hasOwnProperty(t)&&this.controls[t].enabled}setValue(t,e={}){K1(this,!0,t),Object.keys(t).forEach(r=>{Z1(this,!0,r),this.controls[r].setValue(t[r],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(t,e={}){t!=null&&(Object.keys(t).forEach(r=>{let i=this.controls[r];i&&i.patchValue(t[r],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(t={},e={}){this._forEachChild((r,i)=>{r.reset(t?t[i]:null,W(w({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new yd(this))}getRawValue(){return this._reduceChildren({},(t,e,r)=>(t[r]=e.getRawValue(),t))}_syncPendingControls(){let t=this._reduceChildren(!1,(e,r)=>r._syncPendingControls()?!0:e);return t&&this.updateValueAndValidity({onlySelf:!0}),t}_forEachChild(t){Object.keys(this.controls).forEach(e=>{let r=this.controls[e];r&&t(r,e)})}_setUpControls(){this._forEachChild(t=>{t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(t){for(let[e,r]of Object.entries(this.controls))if(this.contains(e)&&t(r))return!0;return!1}_reduceValue(){let t={};return this._reduceChildren(t,(e,r,i)=>((r.enabled||this.disabled)&&(e[i]=r.value),e))}_reduceChildren(t,e){let r=t;return this._forEachChild((i,o)=>{r=e(r,i,o)}),r}_allControlsDisabled(){for(let t of Object.keys(this.controls))if(this.controls[t].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(t){return this.controls.hasOwnProperty(t)?this.controls[t]:null}};var kd=new x("",{factory:()=>xp}),xp="always";function Q1(n,t){return[...t.path,n]}function xd(n,t,e=xp){Dp(n,t),t.valueAccessor.writeValue(n.value),(n.disabled||e==="always")&&t.valueAccessor.setDisabledState?.(n.disabled),J1(n,t),tA(n,t),eA(n,t),X1(n,t)}function Sx(n,t,e=!0){let r=()=>{};t?.valueAccessor?.registerOnChange(r),t?.valueAccessor?.registerOnTouched(r),wd(n,t),n&&(t._invokeOnDestroyCallbacks(),n._registerOnCollectionChange(()=>{}))}function Dd(n,t){n.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(t)})}function X1(n,t){if(t.valueAccessor.setDisabledState){let e=r=>{t.valueAccessor.setDisabledState(r)};n.registerOnDisabledChange(e),t._registerOnDestroy(()=>{n._unregisterOnDisabledChange(e)})}}function Dp(n,t){let e=zx(n);t.validator!==null?n.setValidators(Ex(e,t.validator)):typeof e=="function"&&n.setValidators([e]);let r=Ux(n);t.asyncValidator!==null?n.setAsyncValidators(Ex(r,t.asyncValidator)):typeof r=="function"&&n.setAsyncValidators([r]);let i=()=>n.updateValueAndValidity();Dd(t._rawValidators,i),Dd(t._rawAsyncValidators,i)}function wd(n,t){let e=!1;if(n!==null){if(t.validator!==null){let i=zx(n);if(Array.isArray(i)&&i.length>0){let o=i.filter(s=>s!==t.validator);o.length!==i.length&&(e=!0,n.setValidators(o))}}if(t.asyncValidator!==null){let i=Ux(n);if(Array.isArray(i)&&i.length>0){let o=i.filter(s=>s!==t.asyncValidator);o.length!==i.length&&(e=!0,n.setAsyncValidators(o))}}}let r=()=>{};return Dd(t._rawValidators,r),Dd(t._rawAsyncValidators,r),e}function J1(n,t){t.valueAccessor.registerOnChange(e=>{n._pendingValue=e,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&qx(n,t)})}function eA(n,t){t.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&qx(n,t),n.updateOn!=="submit"&&n.markAsTouched()})}function qx(n,t){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),t.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function tA(n,t){let e=(r,i)=>{t.valueAccessor.writeValue(r),i&&t.viewToModelUpdate(r)};n.registerOnChange(e),t._registerOnDestroy(()=>{n._unregisterOnChange(e)})}function Yx(n,t){n==null,Dp(n,t)}function nA(n,t){return wd(n,t)}function rA(n,t){if(!n.hasOwnProperty("model"))return!1;let e=n.model;return e.isFirstChange()?!0:!Object.is(t,e.currentValue)}function iA(n){return Object.getPrototypeOf(n.constructor)===O1}function Zx(n,t){n._syncPendingControls(),t.forEach(e=>{let r=e.control;r.updateOn==="submit"&&r._pendingChange&&(e.viewToModelUpdate(r._pendingValue),r._pendingChange=!1)})}function oA(n,t){if(!t)return null;Array.isArray(t);let e,r,i;return t.forEach(o=>{o.constructor===Ox?e=o:iA(o)?r=o:i=o}),i||r||e||null}function sA(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}var aA={provide:ji,useExisting:Ut(()=>wp)},ma=Promise.resolve(),wp=(()=>{class n extends ji{callSetDisabledState;get submitted(){return $e(this.submittedReactive)}_submitted=Me(()=>this.submittedReactive());submittedReactive=X(!1);_directives=new Set;form;ngSubmit=new j;options;constructor(e,r,i){super(),this.callSetDisabledState=i,this.form=new vd({},_p(e),vp(r))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){ma.then(()=>{let r=this._findContainer(e.path);e.control=r.registerControl(e.name,e.control),xd(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){ma.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){ma.then(()=>{let r=this._findContainer(e.path),i=new vd({});Yx(i,e),r.registerControl(e.name,i),i.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){ma.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,r){ma.then(()=>{this.form.get(e.path).setValue(r)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),Zx(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new bd(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(r){return new(r||n)(Ne(ga,10),Ne(yp,10),Ne(kd,8))};static \u0275dir=Y({type:n,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(r,i){r&1&&Ce("submit",function(s){return i.onSubmit(s)})("reset",function(){return i.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ke([aA]),Pe]})}return n})();function Tx(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}function Mx(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var Kx=class extends _d{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(t=null,e,r){super(Gx(e),Wx(r,e)),this._applyFormState(t),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Ed(e)&&(e.nonNullable||e.initialValueIsDefault)&&(Mx(t)?this.defaultValue=t.value:this.defaultValue=t)}setValue(t,e={}){this.value=this._pendingValue=t,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(r=>r(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(t,e={}){this.setValue(t,e)}reset(t=this.defaultValue,e={}){this._applyFormState(t),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new yd(this))}_updateValue(){}_anyControls(t){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(t){this._onChange.push(t)}_unregisterOnChange(t){Tx(this._onChange,t)}registerOnDisabledChange(t){this._onDisabledChange.push(t)}_unregisterOnDisabledChange(t){Tx(this._onDisabledChange,t)}_forEachChild(t){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(t){Mx(t)?(this.value=this._pendingValue=t.value,t.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=t}};var cA=n=>n instanceof Kx;var lA={provide:Hi,useExisting:Ut(()=>Cp)},Ax=Promise.resolve(),Cp=(()=>{class n extends Hi{_changeDetectorRef;callSetDisabledState;control=new Kx;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new j;constructor(e,r,i,o,s,a){super(),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(r),this._setAsyncValidators(i),this.valueAccessor=oA(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let r=e.name.previousValue;this.formDirective.removeControl({name:r,path:this._getPath(r)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),rA(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){xd(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){Ax.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let r=e.isDisabled.currentValue,i=r!==0&&re(r);Ax.then(()=>{i&&!this.control.disabled?this.control.disable():!i&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?Q1(e,this._parent):[e]}static \u0275fac=function(r){return new(r||n)(Ne(ji,9),Ne(ga,10),Ne(yp,10),Ne(Cd,10),Ne(tt,8),Ne(kd,8))};static \u0275dir=Y({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Ke([lA]),Pe,it]})}return n})();var dA=(()=>{class n extends ji{callSetDisabledState;get submitted(){return $e(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Me(()=>this._submittedReactive());_submittedReactive=X(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,r,i){super(),this.callSetDisabledState=i,this._setValidators(e),this._setAsyncValidators(r)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(wd(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let r=this.form.get(e.path);return xd(r,e,this.callSetDisabledState),r.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),r}getControl(e){return this.form.get(e.path)}removeControl(e){Sx(e.control||null,e,!1),sA(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,r){this.form.get(e.path).setValue(r)}onReset(){this.resetForm()}resetForm(e=void 0,r={}){this.form.reset(e,r),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,Zx(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new bd(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let r=e.control,i=this.form.get(e.path);r!==i&&(Sx(r||null,e),cA(i)&&(xd(i,e,this.callSetDisabledState),e.control=i))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let r=this.form.get(e.path);Yx(r,e),r.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let r=this.form?.get(e.path);r&&nA(r,e)&&r.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){Dp(this.form,this),this._oldForm&&wd(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(r){return new(r||n)(Ne(ga,10),Ne(yp,10),Ne(kd,8))};static \u0275dir=Y({type:n,features:[Pe,it]})}return n})();var uA={provide:ji,useExisting:Ut(()=>Ep)},Ep=(()=>{class n extends dA{form=null;ngSubmit=new j;get control(){return this.form}static \u0275fac=(()=>{let e;return function(i){return(e||(e=Kt(n)))(i||n)}})();static \u0275dir=Y({type:n,selectors:[["","formGroup",""]],hostBindings:function(r,i){r&1&&Ce("submit",function(s){return i.onSubmit(s)})("reset",function(){return i.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ke([uA]),Pe]})}return n})();var fA=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275mod=je({type:n});static \u0275inj=Be({})}return n})();var Qx=(()=>{class n{static withConfig(e){return{ngModule:n,providers:[{provide:kd,useValue:e.callSetDisabledState??xp}]}}static \u0275fac=function(r){return new(r||n)};static \u0275mod=je({type:n});static \u0275inj=Be({imports:[fA]})}return n})();var Xx=(()=>{class n{_animationsDisabled=Ge();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(r,i){r&2&&Q("mat-pseudo-checkbox-indeterminate",i.state==="indeterminate")("mat-pseudo-checkbox-checked",i.state==="checked")("mat-pseudo-checkbox-disabled",i.disabled)("mat-pseudo-checkbox-minimal",i.appearance==="minimal")("mat-pseudo-checkbox-full",i.appearance==="full")("_mat-animation-noopable",i._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(r,i){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2,changeDetection:0})}return n})();var hA=["text"],pA=[[["mat-icon"]],"*"],gA=["mat-icon","*"];function bA(n,t){if(n&1&&fe(0,"mat-pseudo-checkbox",1),n&2){let e=ne();te("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function yA(n,t){if(n&1&&fe(0,"mat-pseudo-checkbox",3),n&2){let e=ne();te("disabled",e.disabled)}}function _A(n,t){if(n&1&&(I(0,"span",4),_t(1),T()),n&2){let e=ne();S(),en("(",e.group.label,")")}}var Ip=new x("MAT_OPTION_PARENT_COMPONENT"),Sp=new x("MatOptgroup");var kp=class{source;isUserInput;constructor(t,e=!1){this.source=t,this.isUserInput=e}},Id=(()=>{class n{_element=f(z);_changeDetectorRef=f(tt);_parent=f(Ip,{optional:!0});group=f(Sp,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=f(Fe).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=X(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new j;_text;_stateChanges=new E;constructor(){let e=f(vt);e.load(Vr),e.load(Gl),this._signalDisableRipple=!!this._parent&&Nr(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,r){let i=this._getHostElement();typeof i.focus=="function"&&i.focus(r)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!xt(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new kp(this,e))}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-option"]],viewQuery:function(r,i){if(r&1&&et(hA,7),r&2){let o;J(o=ee())&&(i._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(r,i){r&1&&Ce("click",function(){return i._selectViaInteraction()})("keydown",function(s){return i._handleKeydown(s)}),r&2&&(Hn("id",i.id),ue("aria-selected",i.selected)("aria-disabled",i.disabled.toString()),Q("mdc-list-item--selected",i.selected)("mat-mdc-option-multiple",i.multiple)("mat-mdc-option-active",i.active)("mdc-list-item--disabled",i.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",re]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:gA,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(r,i){r&1&&(Ze(pA),pe(0,bA,1,2,"mat-pseudo-checkbox",1),ae(1),I(2,"span",2,0),ae(4,1),T(),pe(5,yA,1,1,"mat-pseudo-checkbox",3),pe(6,_A,2,1,"span",4),fe(7,"div",5)),r&2&&(ge(i.multiple?0:-1),S(5),ge(!i.multiple&&i.selected&&!i.hideSingleSelectionIndicator?5:-1),S(),ge(i.group&&i.group._inert?6:-1),S(),te("matRippleTrigger",i._getHostElement())("matRippleDisabled",i.disabled||i.disableRipple))},dependencies:[Xx,dd],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return n})();function Jx(n,t,e){if(e.length){let r=t.toArray(),i=e.toArray(),o=0;for(let s=0;s<n+1;s++)r[s].group&&r[s].group===i[o]&&o++;return o}return 0}function eD(n,t,e,r){return n<e?n:n+t>e+r?Math.max(0,n-r+t):e}var tD=(()=>{class n{isErrorState(e,r){return!!(e&&e.invalid&&(e.touched||r&&r.submitted))}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Sd=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(t,e,r,i,o){this._defaultMatcher=t,this.ngControl=e,this._parentFormGroup=r,this._parentForm=i,this._stateChanges=o}updateErrorState(){let t=this.errorState,e=this._parentFormGroup||this._parentForm,r=this.matcher||this._defaultMatcher,i=this.ngControl?this.ngControl.control:null,o=r?.isErrorState(i,e)??!1;o!==t&&(this.errorState=o,this._stateChanges.next())}};var vA=["trigger"],xA=["panel"],DA=[[["mat-select-trigger"]],"*"],wA=["mat-select-trigger","*"];function CA(n,t){if(n&1&&(I(0,"span",4),_t(1),T()),n&2){let e=ne();S(),Ti(e.placeholder)}}function EA(n,t){n&1&&ae(0)}function kA(n,t){if(n&1&&(I(0,"span",11),_t(1),T()),n&2){let e=ne(2);S(),Ti(e.triggerValue)}}function IA(n,t){if(n&1&&(I(0,"span",5),pe(1,EA,1,0)(2,kA,2,1,"span",11),T()),n&2){let e=ne();S(),ge(e.customTrigger?1:2)}}function SA(n,t){if(n&1){let e=jn();I(0,"div",12,1),Ce("keydown",function(i){Yt(e);let o=ne();return Zt(o._handleKeydown(i))}),ae(2,1),T()}if(n&2){let e=ne();vn(e.panelClass),Q("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),ue("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var TA=new x("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let n=f(V);return()=>sd(n)}}),MA=new x("MAT_SELECT_CONFIG"),nD=new x("MatSelectTrigger"),Tp=class{source;value;constructor(t,e){this.source=t,this.value=e}},rD=(()=>{class n{_viewportRuler=f(Po);_changeDetectorRef=f(tt);_elementRef=f(z);_dir=f(wn,{optional:!0});_idGenerator=f(Fe);_renderer=f(lt);_parentFormField=f(Kl,{optional:!0});ngControl=f(Hi,{self:!0,optional:!0});_liveAnnouncer=f(Xs);_defaultOptions=f(MA,{optional:!0});_animationsDisabled=Ge();_popoverLocation;_initialized=new E;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let r=this.options.toArray()[e];if(r){let i=this.panel.nativeElement,o=Jx(e,this.options,this.optionGroups),s=r._getHostElement();e===0&&o===1?i.scrollTop=0:i.scrollTop=eD(s.offsetTop,s.offsetHeight,i.scrollTop,i.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new Tp(this,e)}_scrollStrategyFactory=f(TA);_panelOpen=!1;_compareWith=(e,r)=>e===r;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new E;_errorStateTracker;stateChanges=new E;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=X(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(md.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=ei(()=>{let e=this.options;return e?e.changes.pipe(pt(e),ti(()=>an(...e.map(r=>r.onSelectionChange)))):this._initialized.pipe(ti(()=>this.optionSelectionChanges))});openedChange=new j;_openedStream=this.openedChange.pipe(Ie(e=>e),Le(()=>{}));_closedStream=this.openedChange.pipe(Ie(e=>!e),Le(()=>{}));selectionChange=new j;valueChange=new j;constructor(){let e=f(tD),r=f(wp,{optional:!0}),i=f(Ep,{optional:!0}),o=f(new Fr("tabindex"),{optional:!0}),s=f(oa,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Sd(e,this.ngControl,i,r,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new la(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(st(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(st(this._destroy)).subscribe(e=>{e.added.forEach(r=>r.select()),e.removed.forEach(r=>r.deselect())}),this.options.changes.pipe(pt(null),st(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),r=this.ngControl;if(e!==this._triggerAriaLabelledBy){let i=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?i.setAttribute("aria-labelledby",e):i.removeAttribute("aria-labelledby")}r&&(this._previousControl!==r.control&&(this._previousControl!==void 0&&r.disabled!==null&&r.disabled!==this.disabled&&(this.disabled=r.disabled),this._previousControl=r.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Mt(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let r=`${this.id}-panel`;this._trackedModal&&Zh(this._trackedModal,"aria-owns",r),M0(e,"aria-owns",r),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;Zh(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{r(),clearTimeout(i),this._cleanupDetach=void 0};let e=this.panel.nativeElement,r=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),i=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(r=>r.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let r=e.keyCode,i=r===40||r===38||r===37||r===39,o=r===13||r===32,s=this._keyManager;if(!s.isTyping()&&o&&!xt(e)||(this.multiple||e.altKey)&&i)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let c=this.selected;c&&a!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(e){let r=this._keyManager,i=e.keyCode,o=i===40||i===38,s=r.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(i===13||i===32)&&r.activeItem&&!xt(e))e.preventDefault(),r.activeItem._selectViaInteraction();else if(!s&&this._multiple&&i===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(a?c.select():c.deselect())})}else{let a=r.activeItemIndex;r.onKeydown(e),this._multiple&&o&&e.shiftKey&&r.activeItem&&r.activeItemIndex!==a&&r.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!xt(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(r=>r.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(r=>this._selectOptionByValue(r)),this._sortValues();else{let r=this._selectOptionByValue(e);r?this._keyManager.updateActiveItem(r):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let r=this.options.find(i=>{if(this._selectionModel.isSelected(i))return!1;try{return(i.value!=null||this.canSelectNullableOptions)&&this._compareWith(i.value,e)}catch{return!1}});return r&&this._selectionModel.select(r),r}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof Lo?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Js(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=an(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(st(e)).subscribe(r=>{this._onSelect(r.source,r.isUserInput),r.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),an(...this.options.map(r=>r._stateChanges)).pipe(st(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,r){let i=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(i!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),r&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),r&&this.focus())),i!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((r,i)=>this.sortComparator?this.sortComparator(r,i,e):e.indexOf(r)-e.indexOf(i)),this.stateChanges.next()}}_propagateChanges(e){let r;this.multiple?r=this.selected.map(i=>i.value):r=this.selected?this.selected.value:e,this._value=r,this.valueChange.emit(r),this._onChange(r),this.selectionChange.emit(this._getChangeEvent(r)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let r=0;r<this.options.length;r++)if(!this.options.get(r).disabled){e=r;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,r=e?e+" ":"";return this.ariaLabelledby?r+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let r=this._elementRef.nativeElement;e.length?r.setAttribute("aria-describedby",e.join(" ")):r.removeAttribute("aria-describedby")}onContainerClick(e){let r=mt(e);r&&(r.tagName==="MAT-OPTION"||r.classList.contains("cdk-overlay-backdrop")||r.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-select"]],contentQueries:function(r,i,o){if(r&1&&_n(o,nD,5)(o,Id,5)(o,Sp,5),r&2){let s;J(s=ee())&&(i.customTrigger=s.first),J(s=ee())&&(i.options=s),J(s=ee())&&(i.optionGroups=s)}},viewQuery:function(r,i){if(r&1&&et(vA,5)(xA,5)(cd,5),r&2){let o;J(o=ee())&&(i.trigger=o.first),J(o=ee())&&(i.panel=o.first),J(o=ee())&&(i._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(r,i){r&1&&Ce("keydown",function(s){return i._handleKeydown(s)})("focus",function(){return i._onFocus()})("blur",function(){return i._onBlur()}),r&2&&(ue("id",i.id)("tabindex",i.disabled?-1:i.tabIndex)("aria-controls",i.panelOpen?i.id+"-panel":null)("aria-expanded",i.panelOpen)("aria-label",i.ariaLabel||null)("aria-required",i.required.toString())("aria-disabled",i.disabled.toString())("aria-invalid",i.errorState)("aria-activedescendant",i._getAriaActiveDescendant()),Q("mat-mdc-select-disabled",i.disabled)("mat-mdc-select-invalid",i.errorState)("mat-mdc-select-required",i.required)("mat-mdc-select-empty",i.empty)("mat-mdc-select-multiple",i.multiple)("mat-select-open",i.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",re],disableRipple:[2,"disableRipple","disableRipple",re],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:jt(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",re],placeholder:"placeholder",required:[2,"required","required",re],multiple:[2,"multiple","multiple",re],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",re],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",jt],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",re]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Ke([{provide:Zl,useExisting:n},{provide:Ip,useExisting:n}]),it],ngContentSelectors:wA,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(r,i){if(r&1&&(Ze(DA),I(0,"div",2,0),Ce("click",function(){return i.open()}),I(3,"div",3),pe(4,CA,2,1,"span",4)(5,IA,3,1,"span",5),T(),I(6,"div",6)(7,"div",7),mn(),I(8,"svg",8),fe(9,"path",9),T()()()(),Ue(10,SA,3,16,"ng-template",10),Ce("detach",function(){return i.close()})("backdropClick",function(){return i.close()})("overlayKeydown",function(s){return i._handleOverlayKeydown(s)})),r&2){let o=Vt(1);S(3),ue("id",i._valueId),S(),ge(i.empty?4:5),S(6),te("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",i._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",i._scrollStrategy)("cdkConnectedOverlayOrigin",i._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",i._positions)("cdkConnectedOverlayWidth",i._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",i._popoverLocation)}},dependencies:[Lo,cd],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2,changeDetection:0})}return n})(),iD=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["mat-select-trigger"]],features:[Ke([{provide:nD,useExisting:n}])]})}return n})();function AA(n){return!!n&&Object.hasOwn(n,"selectedDocument")}var Mp={selectedDocument:"",documentStates:{}},$o=class n{_userData=X(this.loadUserData());userData=this._userData.asReadonly();loadUserData(){let t=localStorage.getItem("userData");if(!t)return structuredClone(Mp);try{let e=JSON.parse(t);if(AA(e))return e}catch{}return structuredClone(Mp)}saveUserData(t){if(!t){localStorage.setItem("userData",JSON.stringify(Mp));return}localStorage.setItem("userData",JSON.stringify(t)),this._userData.set(t)}static \u0275fac=function(e){return new(e||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})};function RA(n){return Array.isArray(n)&&n.every(({path:t,title:e})=>!!t&&(!!e||e?.trim()===""))}function oD(n,t){let e=structuredClone(n),r=e.variant,i=(()=>{if(r){let s=t.filter(c=>!!c).findIndex(({id:c})=>c===r);if(s===-1)return console.warn(`Document refers to non-existent or already claimed variant: ${e.id} => ${r}`),null;let a=t[s];return oD(a,t)}return null})(),o=e;return i?o.variant=i:delete o.variant,o}var Td=class n{_bossfights=d0(()=>({url:"bossfights/manifest.json",reportProgress:!0}),{parse:t=>{if(!RA(t))throw Error("Manifest is not properly formatted");let e=structuredClone(t);return e.filter(r=>!!r&&!r.isVariant).map(r=>oD(r,e))},defaultValue:null});_rawArray=X(void 0);rawMetadata=this._rawArray.asReadonly();isLoading=this._bossfights.isLoading;error=this._bossfights.error;data=this._bossfights.value;reload(){this._bossfights.reload()}static \u0275fac=function(e){return new(e||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})};var OA=["determinateSpinner"];function NA(n,t){if(n&1&&(mn(),I(0,"svg",11),fe(1,"circle",12),T()),n&2){let e=ne();ue("viewBox",e._viewBox()),S(),cr("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),ue("r",e._circleRadius())}}var PA=new x("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:sD})}),sD=100,FA=10,aD=(()=>{class n{_elementRef=f(z);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=f(PA),r=Xh(),i=this._elementRef.nativeElement;this._noopAnimations=r==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=i.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&r==="reduced-motion"&&i.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=sD;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-FA)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(r,i){if(r&1&&et(OA,5),r&2){let o;J(o=ee())&&(i._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(r,i){r&2&&(ue("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",i.mode==="determinate"?i.value:null)("mode",i.mode),vn("mat-"+i.color),cr("width",i.diameter,"px")("height",i.diameter,"px")("--mat-progress-spinner-size",i.diameter+"px")("--mat-progress-spinner-active-indicator-width",i.diameter+"px"),Q("_mat-animation-noopable",i._noopAnimations)("mdc-circular-progress--indeterminate",i.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",jt],diameter:[2,"diameter","diameter",jt],strokeWidth:[2,"strokeWidth","strokeWidth",jt]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(r,i){if(r&1&&(Ue(0,NA,2,8,"ng-template",null,0,Pr),I(2,"div",2,1),mn(),I(4,"svg",3),fe(5,"circle",4),T()(),go(),I(6,"div",5)(7,"div",6)(8,"div",7),ar(9,8),T(),I(10,"div",9),ar(11,8),T(),I(12,"div",10),ar(13,8),T()()()),r&2){let o=Vt(1);S(4),ue("viewBox",i._viewBox()),S(),cr("stroke-dasharray",i._strokeCircumference(),"px")("stroke-dashoffset",i._strokeDashOffset(),"px")("stroke-width",i._circleStrokeWidth(),"%"),ue("r",i._circleRadius()),S(4),te("ngTemplateOutlet",o),S(2),te("ngTemplateOutlet",o),S(2),te("ngTemplateOutlet",o)}},dependencies:[Ai],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2,changeDetection:0})}return n})();function Np(){return{async:!1,breaks:!1,extensions:null,gfm:!0,hooks:null,pedantic:!1,renderer:null,silent:!1,tokenizer:null,walkTokens:null}}var $i=Np();function pD(n){$i=n}var zi={exec:()=>null};function be(n,t=""){let e=typeof n=="string"?n:n.source,r={replace:(i,o)=>{let s=typeof o=="string"?o:o.source;return s=s.replace(Dt.caret,"$1"),e=e.replace(i,s),r},getRegex:()=>new RegExp(e,t)};return r}var LA=((n="")=>{try{return!!new RegExp("(?<=1)(?<!1)"+n)}catch{return!1}})(),Dt={codeRemoveIndent:/^(?: {1,4}| {0,3}\t)/gm,outputLinkReplace:/\\([\[\]])/g,indentCodeCompensation:/^(\s+)(?:```)/,beginningSpace:/^\s+/,endingHash:/#$/,startingSpaceChar:/^ /,endingSpaceChar:/ $/,nonSpaceChar:/[^ ]/,newLineCharGlobal:/\n/g,tabCharGlobal:/\t/g,multipleSpaceGlobal:/\s+/g,blankLine:/^[ \t]*$/,doubleBlankLine:/\n[ \t]*\n[ \t]*$/,blockquoteStart:/^ {0,3}>/,blockquoteSetextReplace:/\n {0,3}((?:=+|-+) *)(?=\n|$)/g,blockquoteSetextReplace2:/^ {0,3}>[ \t]?/gm,listReplaceNesting:/^ {1,4}(?=( {4})*[^ ])/g,listIsTask:/^\[[ xX]\] +\S/,listReplaceTask:/^\[[ xX]\] +/,listTaskCheckbox:/\[[ xX]\]/,anyLine:/\n.*\n/,hrefBrackets:/^<(.*)>$/,tableDelimiter:/[:|]/,tableAlignChars:/^\||\| *$/g,tableRowBlankLine:/\n[ \t]*$/,tableAlignRight:/^ *-+: *$/,tableAlignCenter:/^ *:-+: *$/,tableAlignLeft:/^ *:-+ *$/,startATag:/^<a /i,endATag:/^<\/a>/i,startPreScriptTag:/^<(pre|code|kbd|script)(\s|>)/i,endPreScriptTag:/^<\/(pre|code|kbd|script)(\s|>)/i,startAngleBracket:/^</,endAngleBracket:/>$/,pedanticHrefTitle:/^([^'"]*[^\s])\s+(['"])(.*)\2/,unicodeAlphaNumeric:/[\p{L}\p{N}]/u,escapeTest:/[&<>"']/,escapeReplace:/[&<>"']/g,escapeTestNoEncode:/[<>"']|&(?!(#\d{1,7}|#[Xx][a-fA-F0-9]{1,6}|\w+);)/,escapeReplaceNoEncode:/[<>"']|&(?!(#\d{1,7}|#[Xx][a-fA-F0-9]{1,6}|\w+);)/g,caret:/(^|[^\[])\^/g,percentDecode:/%25/g,findPipe:/\|/g,splitPipe:/ \|/,slashPipe:/\\\|/g,carriageReturn:/\r\n|\r/g,spaceLine:/^ +$/gm,notSpaceStart:/^\S*/,endingNewline:/\n$/,listItemRegex:n=>new RegExp(`^( {0,3}${n})((?:[	 ][^\\n]*)?(?:\\n|$))`),nextBulletRegex:n=>new RegExp(`^ {0,${Math.min(3,n-1)}}(?:[*+-]|\\d{1,9}[.)])((?:[ 	][^\\n]*)?(?:\\n|$))`),hrRegex:n=>new RegExp(`^ {0,${Math.min(3,n-1)}}((?:- *){3,}|(?:_ *){3,}|(?:\\* *){3,})(?:\\n+|$)`),fencesBeginRegex:n=>new RegExp(`^ {0,${Math.min(3,n-1)}}(?:\`\`\`|~~~)`),headingBeginRegex:n=>new RegExp(`^ {0,${Math.min(3,n-1)}}#`),htmlBeginRegex:n=>new RegExp(`^ {0,${Math.min(3,n-1)}}<(?:[a-z].*>|!--)`,"i"),blockquoteBeginRegex:n=>new RegExp(`^ {0,${Math.min(3,n-1)}}>`)},BA=/^(?:[ \t]*(?:\n|$))+/,VA=/^((?: {4}| {0,3}\t)[^\n]+(?:\n(?:[ \t]*(?:\n|$))*)?)+/,jA=/^ {0,3}(`{3,}(?=[^`\n]*(?:\n|$))|~{3,})([^\n]*)(?:\n|$)(?:|([\s\S]*?)(?:\n|$))(?: {0,3}\1[~`]* *(?=\n|$)|$)/,_a=/^ {0,3}((?:-[\t ]*){3,}|(?:_[ \t]*){3,}|(?:\*[ \t]*){3,})(?:\n+|$)/,HA=/^ {0,3}(#{1,6})(?=\s|$)(.*)(?:\n+|$)/,Pp=/ {0,3}(?:[*+-]|\d{1,9}[.)])/,gD=/^(?!bull |blockCode|fences|blockquote|heading|html|table)((?:.|\n(?!\s*?\n|bull |blockCode|fences|blockquote|heading|html|table))+?)\n {0,3}(=+|-+) *(?:\n+|$)/,bD=be(gD).replace(/bull/g,Pp).replace(/blockCode/g,/(?: {4}| {0,3}\t)/).replace(/fences/g,/ {0,3}(?:`{3,}|~{3,})/).replace(/blockquote/g,/ {0,3}>/).replace(/heading/g,/ {0,3}#{1,6}/).replace(/html/g,/ {0,3}<[^\n>]+>\n/).replace(/\|table/g,"").getRegex(),zA=be(gD).replace(/bull/g,Pp).replace(/blockCode/g,/(?: {4}| {0,3}\t)/).replace(/fences/g,/ {0,3}(?:`{3,}|~{3,})/).replace(/blockquote/g,/ {0,3}>/).replace(/heading/g,/ {0,3}#{1,6}/).replace(/html/g,/ {0,3}<[^\n>]+>\n/).replace(/table/g,/ {0,3}\|?(?:[:\- ]*\|)+[\:\- ]*\n/).getRegex(),Fp=/^([^\n]+(?:\n(?!hr|heading|lheading|blockquote|fences|list|html|table| +\n)[^\n]+)*)/,UA=/^[^\n]+/,Lp=/(?!\s*\])(?:\\[\s\S]|[^\[\]\\])+/,$A=be(/^ {0,3}\[(label)\]: *(?:\n[ \t]*)?([^<\s][^\s]*|<.*?>)(?:(?: +(?:\n[ \t]*)?| *\n[ \t]*)(title))? *(?:\n+|$)/).replace("label",Lp).replace("title",/(?:"(?:\\"?|[^"\\])*"|'[^'\n]*(?:\n[^'\n]+)*\n?'|\([^()]*\))/).getRegex(),GA=be(/^(bull)([ \t][^\n]+?)?(?:\n|$)/).replace(/bull/g,Pp).getRegex(),Nd="address|article|aside|base|basefont|blockquote|body|caption|center|col|colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|footer|form|frame|frameset|h[1-6]|head|header|hr|html|iframe|legend|li|link|main|menu|menuitem|meta|nav|noframes|ol|optgroup|option|p|param|search|section|summary|table|tbody|td|tfoot|th|thead|title|tr|track|ul",Bp=/<!--(?:-?>|[\s\S]*?(?:-->|$))/,WA=be("^ {0,3}(?:<(script|pre|style|textarea)[\\s>][\\s\\S]*?(?:</\\1>[^\\n]*\\n+|$)|comment[^\\n]*(\\n+|$)|<\\?[\\s\\S]*?(?:\\?>\\n*|$)|<![A-Z][\\s\\S]*?(?:>\\n*|$)|<!\\[CDATA\\[[\\s\\S]*?(?:\\]\\]>\\n*|$)|</?(tag)(?: +|\\n|/?>)[\\s\\S]*?(?:(?:\\n[ 	]*)+\\n|$)|<(?!script|pre|style|textarea)([a-z][\\w-]*)(?:attribute)*? */?>(?=[ \\t]*(?:\\n|$))[\\s\\S]*?(?:(?:\\n[ 	]*)+\\n|$)|</(?!script|pre|style|textarea)[a-z][\\w-]*\\s*>(?=[ \\t]*(?:\\n|$))[\\s\\S]*?(?:(?:\\n[ 	]*)+\\n|$))","i").replace("comment",Bp).replace("tag",Nd).replace("attribute",/ +[a-zA-Z:_][\w.:-]*(?: *= *"[^"\n]*"| *= *'[^'\n]*'| *= *[^\s"'=<>`]+)?/).getRegex(),yD=be(Fp).replace("hr",_a).replace("heading"," {0,3}#{1,6}(?:\\s|$)").replace("|lheading","").replace("|table","").replace("blockquote"," {0,3}>").replace("fences"," {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list"," {0,3}(?:[*+-]|1[.)])[ \\t]").replace("html","</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag",Nd).getRegex(),qA=be(/^( {0,3}> ?(paragraph|[^\n]*)(?:\n|$))+/).replace("paragraph",yD).getRegex(),Vp={blockquote:qA,code:VA,def:$A,fences:jA,heading:HA,hr:_a,html:WA,lheading:bD,list:GA,newline:BA,paragraph:yD,table:zi,text:UA},cD=be("^ *([^\\n ].*)\\n {0,3}((?:\\| *)?:?-+:? *(?:\\| *:?-+:? *)*(?:\\| *)?)(?:\\n((?:(?! *\\n|hr|heading|blockquote|code|fences|list|html).*(?:\\n|$))*)\\n*|$)").replace("hr",_a).replace("heading"," {0,3}#{1,6}(?:\\s|$)").replace("blockquote"," {0,3}>").replace("code","(?: {4}| {0,3}	)[^\\n]").replace("fences"," {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list"," {0,3}(?:[*+-]|1[.)])[ \\t]").replace("html","</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag",Nd).getRegex(),YA=W(w({},Vp),{lheading:zA,table:cD,paragraph:be(Fp).replace("hr",_a).replace("heading"," {0,3}#{1,6}(?:\\s|$)").replace("|lheading","").replace("table",cD).replace("blockquote"," {0,3}>").replace("fences"," {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list"," {0,3}(?:[*+-]|1[.)])[ \\t]").replace("html","</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag",Nd).getRegex()}),ZA=W(w({},Vp),{html:be(`^ *(?:comment *(?:\\n|\\s*$)|<(tag)[\\s\\S]+?</\\1> *(?:\\n{2,}|\\s*$)|<tag(?:"[^"]*"|'[^']*'|\\s[^'"/>\\s]*)*?/?> *(?:\\n{2,}|\\s*$))`).replace("comment",Bp).replace(/tag/g,"(?!(?:a|em|strong|small|s|cite|q|dfn|abbr|data|time|code|var|samp|kbd|sub|sup|i|b|u|mark|ruby|rt|rp|bdi|bdo|span|br|wbr|ins|del|img)\\b)\\w+(?!:|[^\\w\\s@]*@)\\b").getRegex(),def:/^ *\[([^\]]+)\]: *<?([^\s>]+)>?(?: +(["(][^\n]+[")]))? *(?:\n+|$)/,heading:/^(#{1,6})(.*)(?:\n+|$)/,fences:zi,lheading:/^(.+?)\n {0,3}(=+|-+) *(?:\n+|$)/,paragraph:be(Fp).replace("hr",_a).replace("heading",` *#{1,6} *[^
]`).replace("lheading",bD).replace("|table","").replace("blockquote"," {0,3}>").replace("|fences","").replace("|list","").replace("|html","").replace("|tag","").getRegex()}),KA=/^\\([!"#$%&'()*+,\-./:;<=>?@\[\]\\^_`{|}~])/,QA=/^(`+)([^`]|[^`][\s\S]*?[^`])\1(?!`)/,_D=/^( {2,}|\\)\n(?!\s*$)/,XA=/^(`+|[^`])(?:(?= {2,}\n)|[\s\S]*?(?:(?=[\\<!\[`*_]|\b_|$)|[^ ](?= {2,}\n)))/,Go=/[\p{P}\p{S}]/u,Pd=/[\s\p{P}\p{S}]/u,jp=/[^\s\p{P}\p{S}]/u,JA=be(/^((?![*_])punctSpace)/,"u").replace(/punctSpace/g,Pd).getRegex(),vD=/(?!~)[\p{P}\p{S}]/u,eR=/(?!~)[\s\p{P}\p{S}]/u,tR=/(?:[^\s\p{P}\p{S}]|~)/u,nR=be(/link|precode-code|html/,"g").replace("link",/\[(?:[^\[\]`]|(?<a>`+)[^`]+\k<a>(?!`))*?\]\((?:\\[\s\S]|[^\\\(\)]|\((?:\\[\s\S]|[^\\\(\)])*\))*\)/).replace("precode-",LA?"(?<!`)()":"(^^|[^`])").replace("code",/(?<b>`+)[^`]+\k<b>(?!`)/).replace("html",/<(?! )[^<>]*?>/).getRegex(),xD=/^(?:\*+(?:((?!\*)punct)|([^\s*]))?)|^_+(?:((?!_)punct)|([^\s_]))?/,rR=be(xD,"u").replace(/punct/g,Go).getRegex(),iR=be(xD,"u").replace(/punct/g,vD).getRegex(),DD="^[^_*]*?__[^_*]*?\\*[^_*]*?(?=__)|[^*]+(?=[^*])|(?!\\*)punct(\\*+)(?=[\\s]|$)|notPunctSpace(\\*+)(?!\\*)(?=punctSpace|$)|(?!\\*)punctSpace(\\*+)(?=notPunctSpace)|[\\s](\\*+)(?!\\*)(?=punct)|(?!\\*)punct(\\*+)(?!\\*)(?=punct)|notPunctSpace(\\*+)(?=notPunctSpace)",oR=be(DD,"gu").replace(/notPunctSpace/g,jp).replace(/punctSpace/g,Pd).replace(/punct/g,Go).getRegex(),sR=be(DD,"gu").replace(/notPunctSpace/g,tR).replace(/punctSpace/g,eR).replace(/punct/g,vD).getRegex(),aR=be("^[^_*]*?\\*\\*[^_*]*?_[^_*]*?(?=\\*\\*)|[^_]+(?=[^_])|(?!_)punct(_+)(?=[\\s]|$)|notPunctSpace(_+)(?!_)(?=punctSpace|$)|(?!_)punctSpace(_+)(?=notPunctSpace)|[\\s](_+)(?!_)(?=punct)|(?!_)punct(_+)(?!_)(?=punct)","gu").replace(/notPunctSpace/g,jp).replace(/punctSpace/g,Pd).replace(/punct/g,Go).getRegex(),cR=be(/^~~?(?:((?!~)punct)|[^\s~])/,"u").replace(/punct/g,Go).getRegex(),lR="^[^~]+(?=[^~])|(?!~)punct(~~?)(?=[\\s]|$)|notPunctSpace(~~?)(?!~)(?=punctSpace|$)|(?!~)punctSpace(~~?)(?=notPunctSpace)|[\\s](~~?)(?!~)(?=punct)|(?!~)punct(~~?)(?!~)(?=punct)|notPunctSpace(~~?)(?=notPunctSpace)",dR=be(lR,"gu").replace(/notPunctSpace/g,jp).replace(/punctSpace/g,Pd).replace(/punct/g,Go).getRegex(),uR=be(/\\(punct)/,"gu").replace(/punct/g,Go).getRegex(),fR=be(/^<(scheme:[^\s\x00-\x1f<>]*|email)>/).replace("scheme",/[a-zA-Z][a-zA-Z0-9+.-]{1,31}/).replace("email",/[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+(@)[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+(?![-_])/).getRegex(),mR=be(Bp).replace("(?:-->|$)","-->").getRegex(),hR=be("^comment|^</[a-zA-Z][\\w:-]*\\s*>|^<[a-zA-Z][\\w-]*(?:attribute)*?\\s*/?>|^<\\?[\\s\\S]*?\\?>|^<![a-zA-Z]+\\s[\\s\\S]*?>|^<!\\[CDATA\\[[\\s\\S]*?\\]\\]>").replace("comment",mR).replace("attribute",/\s+[a-zA-Z:_][\w.:-]*(?:\s*=\s*"[^"]*"|\s*=\s*'[^']*'|\s*=\s*[^\s"'=<>`]+)?/).getRegex(),Ad=/(?:\[(?:\\[\s\S]|[^\[\]\\])*\]|\\[\s\S]|`+(?!`)[^`]*?`+(?!`)|``+(?=\])|[^\[\]\\`])*?/,pR=be(/^!?\[(label)\]\(\s*(href)(?:(?:[ \t]+(?:\n[ \t]*)?|\n[ \t]*)(title))?\s*\)/).replace("label",Ad).replace("href",/<(?:\\.|[^\n<>\\])+>|[^ \t\n\x00-\x1f]*/).replace("title",/"(?:\\"?|[^"\\])*"|'(?:\\'?|[^'\\])*'|\((?:\\\)?|[^)\\])*\)/).getRegex(),wD=be(/^!?\[(label)\]\[(ref)\]/).replace("label",Ad).replace("ref",Lp).getRegex(),CD=be(/^!?\[(ref)\](?:\[\])?/).replace("ref",Lp).getRegex(),gR=be("reflink|nolink(?!\\()","g").replace("reflink",wD).replace("nolink",CD).getRegex(),lD=/[hH][tT][tT][pP][sS]?|[fF][tT][pP]/,Hp={_backpedal:zi,anyPunctuation:uR,autolink:fR,blockSkip:nR,br:_D,code:QA,del:zi,delLDelim:zi,delRDelim:zi,emStrongLDelim:rR,emStrongRDelimAst:oR,emStrongRDelimUnd:aR,escape:KA,link:pR,nolink:CD,punctuation:JA,reflink:wD,reflinkSearch:gR,tag:hR,text:XA,url:zi},bR=W(w({},Hp),{link:be(/^!?\[(label)\]\((.*?)\)/).replace("label",Ad).getRegex(),reflink:be(/^!?\[(label)\]\s*\[([^\]]*)\]/).replace("label",Ad).getRegex()}),Ap=W(w({},Hp),{emStrongRDelimAst:sR,emStrongLDelim:iR,delLDelim:cR,delRDelim:dR,url:be(/^((?:protocol):\/\/|www\.)(?:[a-zA-Z0-9\-]+\.?)+[^\s<]*|^email/).replace("protocol",lD).replace("email",/[A-Za-z0-9._+-]+(@)[a-zA-Z0-9-_]+(?:\.[a-zA-Z0-9-_]*[a-zA-Z0-9])+(?![-_])/).getRegex(),_backpedal:/(?:[^?!.,:;*_'"~()&]+|\([^)]*\)|&(?![a-zA-Z0-9]+;$)|[?!.,:;*_'"~)]+(?!$))+/,del:/^(~~?)(?=[^\s~])((?:\\[\s\S]|[^\\])*?(?:\\[\s\S]|[^\s~\\]))\1(?=[^~]|$)/,text:be(/^([`~]+|[^`~])(?:(?= {2,}\n)|(?=[a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-]+@)|[\s\S]*?(?:(?=[\\<!\[`*~_]|\b_|protocol:\/\/|www\.|$)|[^ ](?= {2,}\n)|[^a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-](?=[a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-]+@)))/).replace("protocol",lD).getRegex()}),yR=W(w({},Ap),{br:be(_D).replace("{2,}","*").getRegex(),text:be(Ap.text).replace("\\b_","\\b_| {2,}\\n").replace(/\{2,\}/g,"*").getRegex()}),Md={normal:Vp,gfm:YA,pedantic:ZA},ba={normal:Hp,gfm:Ap,breaks:yR,pedantic:bR},_R={"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"},dD=n=>_R[n];function $n(n,t){if(t){if(Dt.escapeTest.test(n))return n.replace(Dt.escapeReplace,dD)}else if(Dt.escapeTestNoEncode.test(n))return n.replace(Dt.escapeReplaceNoEncode,dD);return n}function uD(n){try{n=encodeURI(n).replace(Dt.percentDecode,"%")}catch{return null}return n}function fD(n,t){let e=n.replace(Dt.findPipe,(o,s,a)=>{let c=!1,l=s;for(;--l>=0&&a[l]==="\\";)c=!c;return c?"|":" |"}),r=e.split(Dt.splitPipe),i=0;if(r[0].trim()||r.shift(),r.length>0&&!r.at(-1)?.trim()&&r.pop(),t)if(r.length>t)r.splice(t);else for(;r.length<t;)r.push("");for(;i<r.length;i++)r[i]=r[i].trim().replace(Dt.slashPipe,"|");return r}function Hr(n,t,e){let r=n.length;if(r===0)return"";let i=0;for(;i<r;){let o=n.charAt(r-i-1);if(o===t&&!e)i++;else if(o!==t&&e)i++;else break}return n.slice(0,r-i)}function mD(n){let t=n.split(`
`),e=t.length-1;for(;e>=0&&Dt.blankLine.test(t[e]);)e--;return t.length-e<=2?n:t.slice(0,e+1).join(`
`)}function vR(n,t){if(n.indexOf(t[1])===-1)return-1;let e=0;for(let r=0;r<n.length;r++)if(n[r]==="\\")r++;else if(n[r]===t[0])e++;else if(n[r]===t[1]&&(e--,e<0))return r;return e>0?-2:-1}function xR(n,t=0){let e=t,r="";for(let i of n)if(i==="	"){let o=4-e%4;r+=" ".repeat(o),e+=o}else r+=i,e++;return r}function hD(n,t,e,r,i){let o=t.href,s=t.title||null,a=n[1].replace(i.other.outputLinkReplace,"$1");r.state.inLink=!0;let c={type:n[0].charAt(0)==="!"?"image":"link",raw:e,href:o,title:s,text:a,tokens:r.inlineTokens(a)};return r.state.inLink=!1,c}function DR(n,t,e){let r=n.match(e.other.indentCodeCompensation);if(r===null)return t;let i=r[1];return t.split(`
`).map(o=>{let s=o.match(e.other.beginningSpace);if(s===null)return o;let[a]=s;return a.length>=i.length?o.slice(i.length):o}).join(`
`)}var Rd=class{options;rules;lexer;constructor(n){this.options=n||$i}space(n){let t=this.rules.block.newline.exec(n);if(t&&t[0].length>0)return{type:"space",raw:t[0]}}code(n){let t=this.rules.block.code.exec(n);if(t){let e=this.options.pedantic?t[0]:mD(t[0]),r=e.replace(this.rules.other.codeRemoveIndent,"");return{type:"code",raw:e,codeBlockStyle:"indented",text:r}}}fences(n){let t=this.rules.block.fences.exec(n);if(t){let e=t[0],r=DR(e,t[3]||"",this.rules);return{type:"code",raw:e,lang:t[2]?t[2].trim().replace(this.rules.inline.anyPunctuation,"$1"):t[2],text:r}}}heading(n){let t=this.rules.block.heading.exec(n);if(t){let e=t[2].trim();if(this.rules.other.endingHash.test(e)){let r=Hr(e,"#");(this.options.pedantic||!r||this.rules.other.endingSpaceChar.test(r))&&(e=r.trim())}return{type:"heading",raw:Hr(t[0],`
`),depth:t[1].length,text:e,tokens:this.lexer.inline(e)}}}hr(n){let t=this.rules.block.hr.exec(n);if(t)return{type:"hr",raw:Hr(t[0],`
`)}}blockquote(n){let t=this.rules.block.blockquote.exec(n);if(t){let e=Hr(t[0],`
`).split(`
`),r="",i="",o=[];for(;e.length>0;){let s=!1,a=[],c;for(c=0;c<e.length;c++)if(this.rules.other.blockquoteStart.test(e[c]))a.push(e[c]),s=!0;else if(!s)a.push(e[c]);else break;e=e.slice(c);let l=a.join(`
`),d=l.replace(this.rules.other.blockquoteSetextReplace,`
    $1`).replace(this.rules.other.blockquoteSetextReplace2,"");r=r?`${r}
${l}`:l,i=i?`${i}
${d}`:d;let u=this.lexer.state.top;if(this.lexer.state.top=!0,this.lexer.blockTokens(d,o,!0),this.lexer.state.top=u,e.length===0)break;let m=o.at(-1);if(m?.type==="code")break;if(m?.type==="blockquote"){let h=m,p=h.raw+`
`+e.join(`
`),g=this.blockquote(p);o[o.length-1]=g,r=r.substring(0,r.length-h.raw.length)+g.raw,i=i.substring(0,i.length-h.text.length)+g.text;break}else if(m?.type==="list"){let h=m,p=h.raw+`
`+e.join(`
`),g=this.list(p);o[o.length-1]=g,r=r.substring(0,r.length-m.raw.length)+g.raw,i=i.substring(0,i.length-h.raw.length)+g.raw,e=p.substring(o.at(-1).raw.length).split(`
`);continue}}return{type:"blockquote",raw:r,tokens:o,text:i}}}list(n){let t=this.rules.block.list.exec(n);if(t){let e=t[1].trim(),r=e.length>1,i={type:"list",raw:"",ordered:r,start:r?+e.slice(0,-1):"",loose:!1,items:[]};e=r?`\\d{1,9}\\${e.slice(-1)}`:`\\${e}`,this.options.pedantic&&(e=r?e:"[*+-]");let o=this.rules.other.listItemRegex(e),s=!1;for(;n;){let c=!1,l="",d="";if(!(t=o.exec(n))||this.rules.block.hr.test(n))break;l=t[0],n=n.substring(l.length);let u=xR(t[2].split(`
`,1)[0],t[1].length),m=n.split(`
`,1)[0],h=!u.trim(),p=0;if(this.options.pedantic?(p=2,d=u.trimStart()):h?p=t[1].length+1:(p=u.search(this.rules.other.nonSpaceChar),p=p>4?1:p,d=u.slice(p),p+=t[1].length),h&&this.rules.other.blankLine.test(m)&&(l+=m+`
`,n=n.substring(m.length+1),c=!0),!c){let g=this.rules.other.nextBulletRegex(p),y=this.rules.other.hrRegex(p),D=this.rules.other.fencesBeginRegex(p),R=this.rules.other.headingBeginRegex(p),oe=this.rules.other.htmlBeginRegex(p),Z=this.rules.other.blockquoteBeginRegex(p);for(;n;){let we=n.split(`
`,1)[0],me;if(m=we,this.options.pedantic?(m=m.replace(this.rules.other.listReplaceNesting,"  "),me=m):me=m.replace(this.rules.other.tabCharGlobal,"    "),D.test(m)||R.test(m)||oe.test(m)||Z.test(m)||g.test(m)||y.test(m))break;if(me.search(this.rules.other.nonSpaceChar)>=p||!m.trim())d+=`
`+me.slice(p);else{if(h||u.replace(this.rules.other.tabCharGlobal,"    ").search(this.rules.other.nonSpaceChar)>=4||D.test(u)||R.test(u)||y.test(u))break;d+=`
`+m}h=!m.trim(),l+=we+`
`,n=n.substring(we.length+1),u=me.slice(p)}}i.loose||(s?i.loose=!0:this.rules.other.doubleBlankLine.test(l)&&(s=!0)),i.items.push({type:"list_item",raw:l,task:!!this.options.gfm&&this.rules.other.listIsTask.test(d),loose:!1,text:d,tokens:[]}),i.raw+=l}let a=i.items.at(-1);if(a)a.raw=a.raw.trimEnd(),a.text=a.text.trimEnd();else return;i.raw=i.raw.trimEnd();for(let c of i.items){this.lexer.state.top=!1,c.tokens=this.lexer.blockTokens(c.text,[]);let l=c.tokens[0];if(c.task&&(l?.type==="text"||l?.type==="paragraph")){c.text=c.text.replace(this.rules.other.listReplaceTask,""),l.raw=l.raw.replace(this.rules.other.listReplaceTask,""),l.text=l.text.replace(this.rules.other.listReplaceTask,"");for(let u=this.lexer.inlineQueue.length-1;u>=0;u--)if(this.rules.other.listIsTask.test(this.lexer.inlineQueue[u].src)){this.lexer.inlineQueue[u].src=this.lexer.inlineQueue[u].src.replace(this.rules.other.listReplaceTask,"");break}let d=this.rules.other.listTaskCheckbox.exec(c.raw);if(d){let u={type:"checkbox",raw:d[0]+" ",checked:d[0]!=="[ ]"};c.checked=u.checked,i.loose?c.tokens[0]&&["paragraph","text"].includes(c.tokens[0].type)&&"tokens"in c.tokens[0]&&c.tokens[0].tokens?(c.tokens[0].raw=u.raw+c.tokens[0].raw,c.tokens[0].text=u.raw+c.tokens[0].text,c.tokens[0].tokens.unshift(u)):c.tokens.unshift({type:"paragraph",raw:u.raw,text:u.raw,tokens:[u]}):c.tokens.unshift(u)}}else c.task&&(c.task=!1);if(!i.loose){let d=c.tokens.filter(m=>m.type==="space"),u=d.length>0&&d.some(m=>this.rules.other.anyLine.test(m.raw));i.loose=u}}if(i.loose)for(let c of i.items){c.loose=!0;for(let l of c.tokens)l.type==="text"&&(l.type="paragraph")}return i}}html(n){let t=this.rules.block.html.exec(n);if(t){let e=mD(t[0]);return{type:"html",block:!0,raw:e,pre:t[1]==="pre"||t[1]==="script"||t[1]==="style",text:e}}}def(n){let t=this.rules.block.def.exec(n);if(t){let e=t[1].toLowerCase().replace(this.rules.other.multipleSpaceGlobal," "),r=t[2]?t[2].replace(this.rules.other.hrefBrackets,"$1").replace(this.rules.inline.anyPunctuation,"$1"):"",i=t[3]?t[3].substring(1,t[3].length-1).replace(this.rules.inline.anyPunctuation,"$1"):t[3];return{type:"def",tag:e,raw:Hr(t[0],`
`),href:r,title:i}}}table(n){let t=this.rules.block.table.exec(n);if(!t||!this.rules.other.tableDelimiter.test(t[2]))return;let e=fD(t[1]),r=t[2].replace(this.rules.other.tableAlignChars,"").split("|"),i=t[3]?.trim()?t[3].replace(this.rules.other.tableRowBlankLine,"").split(`
`):[],o={type:"table",raw:Hr(t[0],`
`),header:[],align:[],rows:[]};if(e.length===r.length){for(let s of r)this.rules.other.tableAlignRight.test(s)?o.align.push("right"):this.rules.other.tableAlignCenter.test(s)?o.align.push("center"):this.rules.other.tableAlignLeft.test(s)?o.align.push("left"):o.align.push(null);for(let s=0;s<e.length;s++)o.header.push({text:e[s],tokens:this.lexer.inline(e[s]),header:!0,align:o.align[s]});for(let s of i)o.rows.push(fD(s,o.header.length).map((a,c)=>({text:a,tokens:this.lexer.inline(a),header:!1,align:o.align[c]})));return o}}lheading(n){let t=this.rules.block.lheading.exec(n);if(t){let e=t[1].trim();return{type:"heading",raw:Hr(t[0],`
`),depth:t[2].charAt(0)==="="?1:2,text:e,tokens:this.lexer.inline(e)}}}paragraph(n){let t=this.rules.block.paragraph.exec(n);if(t){let e=t[1].charAt(t[1].length-1)===`
`?t[1].slice(0,-1):t[1];return{type:"paragraph",raw:t[0],text:e,tokens:this.lexer.inline(e)}}}text(n){let t=this.rules.block.text.exec(n);if(t)return{type:"text",raw:t[0],text:t[0],tokens:this.lexer.inline(t[0])}}escape(n){let t=this.rules.inline.escape.exec(n);if(t)return{type:"escape",raw:t[0],text:t[1]}}tag(n){let t=this.rules.inline.tag.exec(n);if(t)return!this.lexer.state.inLink&&this.rules.other.startATag.test(t[0])?this.lexer.state.inLink=!0:this.lexer.state.inLink&&this.rules.other.endATag.test(t[0])&&(this.lexer.state.inLink=!1),!this.lexer.state.inRawBlock&&this.rules.other.startPreScriptTag.test(t[0])?this.lexer.state.inRawBlock=!0:this.lexer.state.inRawBlock&&this.rules.other.endPreScriptTag.test(t[0])&&(this.lexer.state.inRawBlock=!1),{type:"html",raw:t[0],inLink:this.lexer.state.inLink,inRawBlock:this.lexer.state.inRawBlock,block:!1,text:t[0]}}link(n){let t=this.rules.inline.link.exec(n);if(t){let e=t[2].trim();if(!this.options.pedantic&&this.rules.other.startAngleBracket.test(e)){if(!this.rules.other.endAngleBracket.test(e))return;let o=Hr(e.slice(0,-1),"\\");if((e.length-o.length)%2===0)return}else{let o=vR(t[2],"()");if(o===-2)return;if(o>-1){let s=(t[0].indexOf("!")===0?5:4)+t[1].length+o;t[2]=t[2].substring(0,o),t[0]=t[0].substring(0,s).trim(),t[3]=""}}let r=t[2],i="";if(this.options.pedantic){let o=this.rules.other.pedanticHrefTitle.exec(r);o&&(r=o[1],i=o[3])}else i=t[3]?t[3].slice(1,-1):"";return r=r.trim(),this.rules.other.startAngleBracket.test(r)&&(this.options.pedantic&&!this.rules.other.endAngleBracket.test(e)?r=r.slice(1):r=r.slice(1,-1)),hD(t,{href:r&&r.replace(this.rules.inline.anyPunctuation,"$1"),title:i&&i.replace(this.rules.inline.anyPunctuation,"$1")},t[0],this.lexer,this.rules)}}reflink(n,t){let e;if((e=this.rules.inline.reflink.exec(n))||(e=this.rules.inline.nolink.exec(n))){let r=(e[2]||e[1]).replace(this.rules.other.multipleSpaceGlobal," "),i=t[r.toLowerCase()];if(!i){let o=e[0].charAt(0);return{type:"text",raw:o,text:o}}return hD(e,i,e[0],this.lexer,this.rules)}}emStrong(n,t,e=""){let r=this.rules.inline.emStrongLDelim.exec(n);if(!(!r||!r[1]&&!r[2]&&!r[3]&&!r[4]||r[4]&&e.match(this.rules.other.unicodeAlphaNumeric))&&(!(r[1]||r[3])||!e||this.rules.inline.punctuation.exec(e))){let i=[...r[0]].length-1,o,s,a=i,c=0,l=r[0][0]==="*"?this.rules.inline.emStrongRDelimAst:this.rules.inline.emStrongRDelimUnd;for(l.lastIndex=0,t=t.slice(-1*n.length+i);(r=l.exec(t))!==null;){if(o=r[1]||r[2]||r[3]||r[4]||r[5]||r[6],!o)continue;if(s=[...o].length,r[3]||r[4]){a+=s;continue}else if((r[5]||r[6])&&i%3&&!((i+s)%3)){c+=s;continue}if(a-=s,a>0)continue;s=Math.min(s,s+a+c);let d=[...r[0]][0].length,u=n.slice(0,i+r.index+d+s);if(Math.min(i,s)%2){let h=u.slice(1,-1);return{type:"em",raw:u,text:h,tokens:this.lexer.inlineTokens(h)}}let m=u.slice(2,-2);return{type:"strong",raw:u,text:m,tokens:this.lexer.inlineTokens(m)}}}}codespan(n){let t=this.rules.inline.code.exec(n);if(t){let e=t[2].replace(this.rules.other.newLineCharGlobal," "),r=this.rules.other.nonSpaceChar.test(e),i=this.rules.other.startingSpaceChar.test(e)&&this.rules.other.endingSpaceChar.test(e);return r&&i&&(e=e.substring(1,e.length-1)),{type:"codespan",raw:t[0],text:e}}}br(n){let t=this.rules.inline.br.exec(n);if(t)return{type:"br",raw:t[0]}}del(n,t,e=""){let r=this.rules.inline.delLDelim.exec(n);if(r&&(!r[1]||!e||this.rules.inline.punctuation.exec(e))){let i=[...r[0]].length-1,o,s,a=i,c=this.rules.inline.delRDelim;for(c.lastIndex=0,t=t.slice(-1*n.length+i);(r=c.exec(t))!==null;){if(o=r[1]||r[2]||r[3]||r[4]||r[5]||r[6],!o||(s=[...o].length,s!==i))continue;if(r[3]||r[4]){a+=s;continue}if(a-=s,a>0)continue;s=Math.min(s,s+a);let l=[...r[0]][0].length,d=n.slice(0,i+r.index+l+s),u=d.slice(i,-i);return{type:"del",raw:d,text:u,tokens:this.lexer.inlineTokens(u)}}}}autolink(n){let t=this.rules.inline.autolink.exec(n);if(t){let e,r;return t[2]==="@"?(e=t[1],r="mailto:"+e):(e=t[1],r=e),{type:"link",raw:t[0],text:e,href:r,tokens:[{type:"text",raw:e,text:e}]}}}url(n){let t;if(t=this.rules.inline.url.exec(n)){let e,r;if(t[2]==="@")e=t[0],r="mailto:"+e;else{let i;do i=t[0],t[0]=this.rules.inline._backpedal.exec(t[0])?.[0]??"";while(i!==t[0]);e=t[0],t[1]==="www."?r="http://"+t[0]:r=t[0]}return{type:"link",raw:t[0],text:e,href:r,tokens:[{type:"text",raw:e,text:e}]}}}inlineText(n){let t=this.rules.inline.text.exec(n);if(t){let e=this.lexer.state.inRawBlock;return{type:"text",raw:t[0],text:t[0],escaped:e}}}},En=class Rp{tokens;options;state;inlineQueue;tokenizer;constructor(t){this.tokens=[],this.tokens.links=Object.create(null),this.options=t||$i,this.options.tokenizer=this.options.tokenizer||new Rd,this.tokenizer=this.options.tokenizer,this.tokenizer.options=this.options,this.tokenizer.lexer=this,this.inlineQueue=[],this.state={inLink:!1,inRawBlock:!1,top:!0};let e={other:Dt,block:Md.normal,inline:ba.normal};this.options.pedantic?(e.block=Md.pedantic,e.inline=ba.pedantic):this.options.gfm&&(e.block=Md.gfm,this.options.breaks?e.inline=ba.breaks:e.inline=ba.gfm),this.tokenizer.rules=e}static get rules(){return{block:Md,inline:ba}}static lex(t,e){return new Rp(e).lex(t)}static lexInline(t,e){return new Rp(e).inlineTokens(t)}lex(t){t=t.replace(Dt.carriageReturn,`
`),this.blockTokens(t,this.tokens);for(let e=0;e<this.inlineQueue.length;e++){let r=this.inlineQueue[e];this.inlineTokens(r.src,r.tokens)}return this.inlineQueue=[],this.tokens}blockTokens(t,e=[],r=!1){this.tokenizer.lexer=this,this.options.pedantic&&(t=t.replace(Dt.tabCharGlobal,"    ").replace(Dt.spaceLine,""));let i=1/0;for(;t;){if(t.length<i)i=t.length;else{this.infiniteLoopError(t.charCodeAt(0));break}let o;if(this.options.extensions?.block?.some(a=>(o=a.call({lexer:this},t,e))?(t=t.substring(o.raw.length),e.push(o),!0):!1))continue;if(o=this.tokenizer.space(t)){t=t.substring(o.raw.length);let a=e.at(-1);o.raw.length===1&&a!==void 0?a.raw+=`
`:e.push(o);continue}if(o=this.tokenizer.code(t)){t=t.substring(o.raw.length);let a=e.at(-1);a?.type==="paragraph"||a?.type==="text"?(a.raw+=(a.raw.endsWith(`
`)?"":`
`)+o.raw,a.text+=`
`+o.text,this.inlineQueue.at(-1).src=a.text):e.push(o);continue}if(o=this.tokenizer.fences(t)){t=t.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.heading(t)){t=t.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.hr(t)){t=t.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.blockquote(t)){t=t.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.list(t)){t=t.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.html(t)){t=t.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.def(t)){t=t.substring(o.raw.length);let a=e.at(-1);a?.type==="paragraph"||a?.type==="text"?(a.raw+=(a.raw.endsWith(`
`)?"":`
`)+o.raw,a.text+=`
`+o.raw,this.inlineQueue.at(-1).src=a.text):this.tokens.links[o.tag]||(this.tokens.links[o.tag]={href:o.href,title:o.title},e.push(o));continue}if(o=this.tokenizer.table(t)){t=t.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.lheading(t)){t=t.substring(o.raw.length),e.push(o);continue}let s=t;if(this.options.extensions?.startBlock){let a=1/0,c=t.slice(1),l;this.options.extensions.startBlock.forEach(d=>{l=d.call({lexer:this},c),typeof l=="number"&&l>=0&&(a=Math.min(a,l))}),a<1/0&&a>=0&&(s=t.substring(0,a+1))}if(this.state.top&&(o=this.tokenizer.paragraph(s))){let a=e.at(-1);r&&a?.type==="paragraph"?(a.raw+=(a.raw.endsWith(`
`)?"":`
`)+o.raw,a.text+=`
`+o.text,this.inlineQueue.pop(),this.inlineQueue.at(-1).src=a.text):e.push(o),r=s.length!==t.length,t=t.substring(o.raw.length);continue}if(o=this.tokenizer.text(t)){t=t.substring(o.raw.length);let a=e.at(-1);a?.type==="text"?(a.raw+=(a.raw.endsWith(`
`)?"":`
`)+o.raw,a.text+=`
`+o.text,this.inlineQueue.pop(),this.inlineQueue.at(-1).src=a.text):e.push(o);continue}if(t){this.infiniteLoopError(t.charCodeAt(0));break}}return this.state.top=!0,e}inline(t,e=[]){return this.inlineQueue.push({src:t,tokens:e}),e}inlineTokens(t,e=[]){this.tokenizer.lexer=this;let r=t,i=null;if(this.tokens.links){let l=Object.keys(this.tokens.links);if(l.length>0)for(;(i=this.tokenizer.rules.inline.reflinkSearch.exec(r))!==null;)l.includes(i[0].slice(i[0].lastIndexOf("[")+1,-1))&&(r=r.slice(0,i.index)+"["+"a".repeat(i[0].length-2)+"]"+r.slice(this.tokenizer.rules.inline.reflinkSearch.lastIndex))}for(;(i=this.tokenizer.rules.inline.anyPunctuation.exec(r))!==null;)r=r.slice(0,i.index)+"++"+r.slice(this.tokenizer.rules.inline.anyPunctuation.lastIndex);let o;for(;(i=this.tokenizer.rules.inline.blockSkip.exec(r))!==null;)o=i[2]?i[2].length:0,r=r.slice(0,i.index+o)+"["+"a".repeat(i[0].length-o-2)+"]"+r.slice(this.tokenizer.rules.inline.blockSkip.lastIndex);r=this.options.hooks?.emStrongMask?.call({lexer:this},r)??r;let s=!1,a="",c=1/0;for(;t;){if(t.length<c)c=t.length;else{this.infiniteLoopError(t.charCodeAt(0));break}s||(a=""),s=!1;let l;if(this.options.extensions?.inline?.some(u=>(l=u.call({lexer:this},t,e))?(t=t.substring(l.raw.length),e.push(l),!0):!1))continue;if(l=this.tokenizer.escape(t)){t=t.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.tag(t)){t=t.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.link(t)){t=t.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.reflink(t,this.tokens.links)){t=t.substring(l.raw.length);let u=e.at(-1);l.type==="text"&&u?.type==="text"?(u.raw+=l.raw,u.text+=l.text):e.push(l);continue}if(l=this.tokenizer.emStrong(t,r,a)){t=t.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.codespan(t)){t=t.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.br(t)){t=t.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.del(t,r,a)){t=t.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.autolink(t)){t=t.substring(l.raw.length),e.push(l);continue}if(!this.state.inLink&&(l=this.tokenizer.url(t))){t=t.substring(l.raw.length),e.push(l);continue}let d=t;if(this.options.extensions?.startInline){let u=1/0,m=t.slice(1),h;this.options.extensions.startInline.forEach(p=>{h=p.call({lexer:this},m),typeof h=="number"&&h>=0&&(u=Math.min(u,h))}),u<1/0&&u>=0&&(d=t.substring(0,u+1))}if(l=this.tokenizer.inlineText(d)){t=t.substring(l.raw.length),l.raw.slice(-1)!=="_"&&(a=l.raw.slice(-1)),s=!0;let u=e.at(-1);u?.type==="text"?(u.raw+=l.raw,u.text+=l.text):e.push(l);continue}if(t){this.infiniteLoopError(t.charCodeAt(0));break}}return e}infiniteLoopError(t){let e="Infinite loop on byte: "+t;if(this.options.silent)console.error(e);else throw new Error(e)}},Od=class{options;parser;constructor(n){this.options=n||$i}space(n){return""}code({text:n,lang:t,escaped:e}){let r=(t||"").match(Dt.notSpaceStart)?.[0],i=n.replace(Dt.endingNewline,"")+`
`;return r?'<pre><code class="language-'+$n(r)+'">'+(e?i:$n(i,!0))+`</code></pre>
`:"<pre><code>"+(e?i:$n(i,!0))+`</code></pre>
`}blockquote({tokens:n}){return`<blockquote>
${this.parser.parse(n)}</blockquote>
`}html({text:n}){return n}def(n){return""}heading({tokens:n,depth:t}){return`<h${t}>${this.parser.parseInline(n)}</h${t}>
`}hr(n){return`<hr>
`}list(n){let t=n.ordered,e=n.start,r="";for(let s=0;s<n.items.length;s++){let a=n.items[s];r+=this.listitem(a)}let i=t?"ol":"ul",o=t&&e!==1?' start="'+e+'"':"";return"<"+i+o+`>
`+r+"</"+i+`>
`}listitem(n){return`<li>${this.parser.parse(n.tokens)}</li>
`}checkbox({checked:n}){return"<input "+(n?'checked="" ':"")+'disabled="" type="checkbox"> '}paragraph({tokens:n}){return`<p>${this.parser.parseInline(n)}</p>
`}table(n){let t="",e="";for(let i=0;i<n.header.length;i++)e+=this.tablecell(n.header[i]);t+=this.tablerow({text:e});let r="";for(let i=0;i<n.rows.length;i++){let o=n.rows[i];e="";for(let s=0;s<o.length;s++)e+=this.tablecell(o[s]);r+=this.tablerow({text:e})}return r&&(r=`<tbody>${r}</tbody>`),`<table>
<thead>
`+t+`</thead>
`+r+`</table>
`}tablerow({text:n}){return`<tr>
${n}</tr>
`}tablecell(n){let t=this.parser.parseInline(n.tokens),e=n.header?"th":"td";return(n.align?`<${e} align="${n.align}">`:`<${e}>`)+t+`</${e}>
`}strong({tokens:n}){return`<strong>${this.parser.parseInline(n)}</strong>`}em({tokens:n}){return`<em>${this.parser.parseInline(n)}</em>`}codespan({text:n}){return`<code>${$n(n,!0)}</code>`}br(n){return"<br>"}del({tokens:n}){return`<del>${this.parser.parseInline(n)}</del>`}link({href:n,title:t,tokens:e}){let r=this.parser.parseInline(e),i=uD(n);if(i===null)return r;n=i;let o='<a href="'+n+'"';return t&&(o+=' title="'+$n(t)+'"'),o+=">"+r+"</a>",o}image({href:n,title:t,text:e,tokens:r}){r&&(e=this.parser.parseInline(r,this.parser.textRenderer));let i=uD(n);if(i===null)return $n(e);n=i;let o=`<img src="${n}" alt="${$n(e)}"`;return t&&(o+=` title="${$n(t)}"`),o+=">",o}text(n){return"tokens"in n&&n.tokens?this.parser.parseInline(n.tokens):"escaped"in n&&n.escaped?n.text:$n(n.text)}},zp=class{strong({text:n}){return n}em({text:n}){return n}codespan({text:n}){return n}del({text:n}){return n}html({text:n}){return n}text({text:n}){return n}link({text:n}){return""+n}image({text:n}){return""+n}br(){return""}checkbox({raw:n}){return n}},kn=class Op{options;renderer;textRenderer;constructor(t){this.options=t||$i,this.options.renderer=this.options.renderer||new Od,this.renderer=this.options.renderer,this.renderer.options=this.options,this.renderer.parser=this,this.textRenderer=new zp}static parse(t,e){return new Op(e).parse(t)}static parseInline(t,e){return new Op(e).parseInline(t)}parse(t){this.renderer.parser=this;let e="";for(let r=0;r<t.length;r++){let i=t[r];if(this.options.extensions?.renderers?.[i.type]){let s=i,a=this.options.extensions.renderers[s.type].call({parser:this},s);if(a!==!1||!["space","hr","heading","code","table","blockquote","list","html","def","paragraph","text"].includes(s.type)){e+=a||"";continue}}let o=i;switch(o.type){case"space":{e+=this.renderer.space(o);break}case"hr":{e+=this.renderer.hr(o);break}case"heading":{e+=this.renderer.heading(o);break}case"code":{e+=this.renderer.code(o);break}case"table":{e+=this.renderer.table(o);break}case"blockquote":{e+=this.renderer.blockquote(o);break}case"list":{e+=this.renderer.list(o);break}case"checkbox":{e+=this.renderer.checkbox(o);break}case"html":{e+=this.renderer.html(o);break}case"def":{e+=this.renderer.def(o);break}case"paragraph":{e+=this.renderer.paragraph(o);break}case"text":{e+=this.renderer.text(o);break}default:{let s='Token with "'+o.type+'" type was not found.';if(this.options.silent)return console.error(s),"";throw new Error(s)}}}return e}parseInline(t,e=this.renderer){this.renderer.parser=this;let r="";for(let i=0;i<t.length;i++){let o=t[i];if(this.options.extensions?.renderers?.[o.type]){let a=this.options.extensions.renderers[o.type].call({parser:this},o);if(a!==!1||!["escape","html","link","image","strong","em","codespan","br","del","text"].includes(o.type)){r+=a||"";continue}}let s=o;switch(s.type){case"escape":{r+=e.text(s);break}case"html":{r+=e.html(s);break}case"link":{r+=e.link(s);break}case"image":{r+=e.image(s);break}case"checkbox":{r+=e.checkbox(s);break}case"strong":{r+=e.strong(s);break}case"em":{r+=e.em(s);break}case"codespan":{r+=e.codespan(s);break}case"br":{r+=e.br(s);break}case"del":{r+=e.del(s);break}case"text":{r+=e.text(s);break}default:{let a='Token with "'+s.type+'" type was not found.';if(this.options.silent)return console.error(a),"";throw new Error(a)}}}return r}},ya=class{options;block;constructor(n){this.options=n||$i}static passThroughHooks=new Set(["preprocess","postprocess","processAllTokens","emStrongMask"]);static passThroughHooksRespectAsync=new Set(["preprocess","postprocess","processAllTokens"]);preprocess(n){return n}postprocess(n){return n}processAllTokens(n){return n}emStrongMask(n){return n}provideLexer(n=this.block){return n?En.lex:En.lexInline}provideParser(n=this.block){return n?kn.parse:kn.parseInline}},wR=class{defaults=Np();options=this.setOptions;parse=this.parseMarkdown(!0);parseInline=this.parseMarkdown(!1);Parser=kn;Renderer=Od;TextRenderer=zp;Lexer=En;Tokenizer=Rd;Hooks=ya;constructor(...n){this.use(...n)}walkTokens(n,t){let e=[];for(let r of n)switch(e=e.concat(t.call(this,r)),r.type){case"table":{let i=r;for(let o of i.header)e=e.concat(this.walkTokens(o.tokens,t));for(let o of i.rows)for(let s of o)e=e.concat(this.walkTokens(s.tokens,t));break}case"list":{let i=r;e=e.concat(this.walkTokens(i.items,t));break}default:{let i=r;this.defaults.extensions?.childTokens?.[i.type]?this.defaults.extensions.childTokens[i.type].forEach(o=>{let s=i[o].flat(1/0);e=e.concat(this.walkTokens(s,t))}):i.tokens&&(e=e.concat(this.walkTokens(i.tokens,t)))}}return e}use(...n){let t=this.defaults.extensions||{renderers:{},childTokens:{}};return n.forEach(e=>{let r=w({},e);if(r.async=this.defaults.async||r.async||!1,e.extensions&&(e.extensions.forEach(i=>{if(!i.name)throw new Error("extension name required");if("renderer"in i){let o=t.renderers[i.name];o?t.renderers[i.name]=function(...s){let a=i.renderer.apply(this,s);return a===!1&&(a=o.apply(this,s)),a}:t.renderers[i.name]=i.renderer}if("tokenizer"in i){if(!i.level||i.level!=="block"&&i.level!=="inline")throw new Error("extension level must be 'block' or 'inline'");let o=t[i.level];o?o.unshift(i.tokenizer):t[i.level]=[i.tokenizer],i.start&&(i.level==="block"?t.startBlock?t.startBlock.push(i.start):t.startBlock=[i.start]:i.level==="inline"&&(t.startInline?t.startInline.push(i.start):t.startInline=[i.start]))}"childTokens"in i&&i.childTokens&&(t.childTokens[i.name]=i.childTokens)}),r.extensions=t),e.renderer){let i=this.defaults.renderer||new Od(this.defaults);for(let o in e.renderer){if(!(o in i))throw new Error(`renderer '${o}' does not exist`);if(["options","parser"].includes(o))continue;let s=o,a=e.renderer[s],c=i[s];i[s]=(...l)=>{let d=a.apply(i,l);return d===!1&&(d=c.apply(i,l)),d||""}}r.renderer=i}if(e.tokenizer){let i=this.defaults.tokenizer||new Rd(this.defaults);for(let o in e.tokenizer){if(!(o in i))throw new Error(`tokenizer '${o}' does not exist`);if(["options","rules","lexer"].includes(o))continue;let s=o,a=e.tokenizer[s],c=i[s];i[s]=(...l)=>{let d=a.apply(i,l);return d===!1&&(d=c.apply(i,l)),d}}r.tokenizer=i}if(e.hooks){let i=this.defaults.hooks||new ya;for(let o in e.hooks){if(!(o in i))throw new Error(`hook '${o}' does not exist`);if(["options","block"].includes(o))continue;let s=o,a=e.hooks[s],c=i[s];ya.passThroughHooks.has(o)?i[s]=l=>{if(this.defaults.async&&ya.passThroughHooksRespectAsync.has(o))return(async()=>{let u=await a.call(i,l);return c.call(i,u)})();let d=a.call(i,l);return c.call(i,d)}:i[s]=(...l)=>{if(this.defaults.async)return(async()=>{let u=await a.apply(i,l);return u===!1&&(u=await c.apply(i,l)),u})();let d=a.apply(i,l);return d===!1&&(d=c.apply(i,l)),d}}r.hooks=i}if(e.walkTokens){let i=this.defaults.walkTokens,o=e.walkTokens;r.walkTokens=function(s){let a=[];return a.push(o.call(this,s)),i&&(a=a.concat(i.call(this,s))),a}}this.defaults=w(w({},this.defaults),r)}),this}setOptions(n){return this.defaults=w(w({},this.defaults),n),this}lexer(n,t){return En.lex(n,t??this.defaults)}parser(n,t){return kn.parse(n,t??this.defaults)}parseMarkdown(n){return(t,e)=>{let r=w({},e),i=w(w({},this.defaults),r),o=this.onError(!!i.silent,!!i.async);if(this.defaults.async===!0&&r.async===!1)return o(new Error("marked(): The async option was set to true by an extension. Remove async: false from the parse options object to return a Promise."));if(typeof t>"u"||t===null)return o(new Error("marked(): input parameter is undefined or null"));if(typeof t!="string")return o(new Error("marked(): input parameter is of type "+Object.prototype.toString.call(t)+", string expected"));if(i.hooks&&(i.hooks.options=i,i.hooks.block=n),i.async)return(async()=>{let s=i.hooks?await i.hooks.preprocess(t):t,a=await(i.hooks?await i.hooks.provideLexer(n):n?En.lex:En.lexInline)(s,i),c=i.hooks?await i.hooks.processAllTokens(a):a;i.walkTokens&&await Promise.all(this.walkTokens(c,i.walkTokens));let l=await(i.hooks?await i.hooks.provideParser(n):n?kn.parse:kn.parseInline)(c,i);return i.hooks?await i.hooks.postprocess(l):l})().catch(o);try{i.hooks&&(t=i.hooks.preprocess(t));let s=(i.hooks?i.hooks.provideLexer(n):n?En.lex:En.lexInline)(t,i);i.hooks&&(s=i.hooks.processAllTokens(s)),i.walkTokens&&this.walkTokens(s,i.walkTokens);let a=(i.hooks?i.hooks.provideParser(n):n?kn.parse:kn.parseInline)(s,i);return i.hooks&&(a=i.hooks.postprocess(a)),a}catch(s){return o(s)}}}onError(n,t){return e=>{if(e.message+=`
Please report this to https://github.com/markedjs/marked.`,n){let r="<p>An error occurred:</p><pre>"+$n(e.message+"",!0)+"</pre>";return t?Promise.resolve(r):r}if(t)return Promise.reject(e);throw e}}},Ui=new wR;function De(n,t){return Ui.parse(n,t)}De.options=De.setOptions=function(n){return Ui.setOptions(n),De.defaults=Ui.defaults,pD(De.defaults),De};De.getDefaults=Np;De.defaults=$i;De.use=function(...n){return Ui.use(...n),De.defaults=Ui.defaults,pD(De.defaults),De};De.walkTokens=function(n,t){return Ui.walkTokens(n,t)};De.parseInline=Ui.parseInline;De.Parser=kn;De.parser=kn.parse;De.Renderer=Od;De.TextRenderer=zp;De.Lexer=En;De.lexer=En.lex;De.Tokenizer=Rd;De.Hooks=ya;De.parse=De;var zY=De.options,UY=De.setOptions,$Y=De.use,GY=De.walkTokens,WY=De.parseInline;var qY=kn.parse,YY=En.lex;var Fd=class n{SPOILER_PATTERN=/\|\|(.+?)\|\|/g;parseMarkdownDocument(t){let e=De.lexer(t),r=[],i="";for(let o of e)switch(o.type){case"heading":{let s=o;i=s.text;let a=s.raw.replace(/^#{1,6}\s+/,"");r.push({type:"heading",level:s.depth,html:De.parseInline(a),plainText:s.text});break}case"html":{r.push({type:"inline-html",html:o.raw});break}case"list":{let a=o.items.map((c,l)=>{let d=this.getRawText(c);return this.parseListItem(d,i,l)});r.push({type:"list",items:a});break}case"paragraph":{let s=o;r.push({type:"paragraph",html:De.parseInline(s.raw)});break}case"hr":case"thematic_break":{r.push({type:"thematic-break"});break}default:break}return r}getRawText(t){return t.tokens?t.tokens.map(e=>e.raw).join(""):t.raw||""}parseListItem(t,e,r){let i=[],o=t.matchAll(this.SPOILER_PATTERN);for(let c of o)i.push(c[1].replaceAll("`","").trim());let s=t.replace(this.SPOILER_PATTERN,"").trim();return s=s.replace(/\s+/g," ").trim(),{id:this.listItemId(e,s,r),text:s,spoilers:i}}listItemId(t,e,r){return`li-${this.hash(`${t}||${e}||${r}`)}`}hash(t){let e=5381;for(let r=0;r<t.length;r++)e=(e<<5)+e+t.charCodeAt(r),e=e&e;return Math.abs(e).toString(36).padStart(6,"0")}static \u0275fac=function(e){return new(e||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})};var Up=(()=>{class n{get vertical(){return this._vertical}set vertical(e){this._vertical=ta(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=ta(e)}_inset=!1;static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(r,i){r&2&&(ue("aria-orientation",i.vertical?"vertical":"horizontal"),Q("mat-divider-vertical",i.vertical)("mat-divider-horizontal",!i.vertical)("mat-divider-inset",i.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(r,i){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-top-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-right-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2,changeDetection:0})}return n})();var $p=new x("CdkAccordion"),ED=(()=>{class n{_stateChanges=new E;_openCloseAllActions=new E;id=f(Fe).getId("cdk-accordion-");multi=!1;openAll(){this.multi&&this._openCloseAllActions.next(!0)}closeAll(){this._openCloseAllActions.next(!1)}ngOnChanges(e){this._stateChanges.next(e)}ngOnDestroy(){this._stateChanges.complete(),this._openCloseAllActions.complete()}static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["cdk-accordion"],["","cdkAccordion",""]],inputs:{multi:[2,"multi","multi",re]},exportAs:["cdkAccordion"],features:[Ke([{provide:$p,useExisting:n}]),it]})}return n})(),kD=(()=>{class n{accordion=f($p,{optional:!0,skipSelf:!0});_changeDetectorRef=f(tt);_expansionDispatcher=f(da);_openCloseAllSubscription=ye.EMPTY;closed=new j;opened=new j;destroyed=new j;expandedChange=new j;id=f(Fe).getId("cdk-accordion-child-");get expanded(){return this._expanded}set expanded(e){if(this._expanded!==e){if(this._expanded=e,this.expandedChange.emit(e),e){this.opened.emit();let r=this.accordion?this.accordion.id:this.id;this._expansionDispatcher.notify(this.id,r)}else this.closed.emit();this._changeDetectorRef.markForCheck()}}_expanded=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=X(!1);_removeUniqueSelectionListener=()=>{};constructor(){}ngOnInit(){this._removeUniqueSelectionListener=this._expansionDispatcher.listen((e,r)=>{this.accordion&&!this.accordion.multi&&this.accordion.id===r&&this.id!==e&&(this.expanded=!1)}),this.accordion&&(this._openCloseAllSubscription=this._subscribeToOpenCloseAllActions())}ngOnDestroy(){this.opened.complete(),this.closed.complete(),this.destroyed.emit(),this.destroyed.complete(),this._removeUniqueSelectionListener(),this._openCloseAllSubscription.unsubscribe()}toggle(){this.disabled||(this.expanded=!this.expanded)}close(){this.disabled||(this.expanded=!1)}open(){this.disabled||(this.expanded=!0)}_subscribeToOpenCloseAllActions(){return this.accordion._openCloseAllActions.subscribe(e=>{this.disabled||(this.expanded=e)})}static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["cdk-accordion-item"],["","cdkAccordionItem",""]],inputs:{expanded:[2,"expanded","expanded",re],disabled:[2,"disabled","disabled",re]},outputs:{closed:"closed",opened:"opened",destroyed:"destroyed",expandedChange:"expandedChange"},exportAs:["cdkAccordionItem"],features:[Ke([{provide:$p,useValue:void 0}])]})}return n})();var CR=["body"],ER=["bodyWrapper"],kR=[[["mat-expansion-panel-header"]],"*",[["mat-action-row"]]],IR=["mat-expansion-panel-header","*","mat-action-row"];function SR(n,t){}var TR=[[["mat-panel-title"]],[["mat-panel-description"]],"*"],MR=["mat-panel-title","mat-panel-description","*"];function AR(n,t){n&1&&(Bt(0,"span",1),mn(),Bt(1,"svg",2),Jt(2,"path",3),Xt()())}var Gp=new x("MAT_ACCORDION"),ID=new x("MAT_EXPANSION_PANEL"),RR=(()=>{class n{_template=f(yt);_expansionPanel=f(ID,{optional:!0});constructor(){}static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["ng-template","matExpansionPanelContent",""]]})}return n})(),SD=new x("MAT_EXPANSION_PANEL_DEFAULT_OPTIONS"),Wp=(()=>{class n extends kD{_viewContainerRef=f(Lt);_animationsDisabled=Ge();_document=f(B);_ngZone=f(F);_elementRef=f(z);_renderer=f(lt);_cleanupTransitionEnd;get hideToggle(){return this._hideToggle||this.accordion&&this.accordion.hideToggle}set hideToggle(e){this._hideToggle=e}_hideToggle=!1;get togglePosition(){return this._togglePosition||this.accordion&&this.accordion.togglePosition}set togglePosition(e){this._togglePosition=e}_togglePosition;afterExpand=new j;afterCollapse=new j;_inputChanges=new E;accordion=f(Gp,{optional:!0,skipSelf:!0});_lazyContent;_body;_bodyWrapper;_portal;_headerId=f(Fe).getId("mat-expansion-panel-header-");constructor(){super();let e=f(SD,{optional:!0});this._expansionDispatcher=f(da),e&&(this.hideToggle=e.hideToggle)}_hasSpacing(){return this.accordion?this.expanded&&this.accordion.displayMode==="default":!1}_getExpandedState(){return this.expanded?"expanded":"collapsed"}toggle(){this.expanded=!this.expanded}close(){this.expanded=!1}open(){this.expanded=!0}ngAfterContentInit(){this._lazyContent&&this._lazyContent._expansionPanel===this&&this.opened.pipe(pt(null),Ie(()=>this.expanded&&!this._portal),Mt(1)).subscribe(()=>{this._portal=new Cn(this._lazyContent._template,this._viewContainerRef)}),this._setupAnimationEvents()}ngOnChanges(e){this._inputChanges.next(e)}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransitionEnd?.(),this._inputChanges.complete()}_containsFocus(){if(this._body){let e=this._document.activeElement,r=this._body.nativeElement;return e===r||r.contains(e)}return!1}_transitionEndListener=({target:e,propertyName:r})=>{e===this._bodyWrapper?.nativeElement&&r==="grid-template-rows"&&this._ngZone.run(()=>{this.expanded?this.afterExpand.emit():this.afterCollapse.emit()})};_setupAnimationEvents(){this._ngZone.runOutsideAngular(()=>{this._animationsDisabled?(this.opened.subscribe(()=>this._ngZone.run(()=>this.afterExpand.emit())),this.closed.subscribe(()=>this._ngZone.run(()=>this.afterCollapse.emit()))):setTimeout(()=>{let e=this._elementRef.nativeElement;this._cleanupTransitionEnd=this._renderer.listen(e,"transitionend",this._transitionEndListener),e.classList.add("mat-expansion-panel-animations-enabled")},200)})}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-expansion-panel"]],contentQueries:function(r,i,o){if(r&1&&_n(o,RR,5),r&2){let s;J(s=ee())&&(i._lazyContent=s.first)}},viewQuery:function(r,i){if(r&1&&et(CR,5)(ER,5),r&2){let o;J(o=ee())&&(i._body=o.first),J(o=ee())&&(i._bodyWrapper=o.first)}},hostAttrs:[1,"mat-expansion-panel"],hostVars:4,hostBindings:function(r,i){r&2&&Q("mat-expanded",i.expanded)("mat-expansion-panel-spacing",i._hasSpacing())},inputs:{hideToggle:[2,"hideToggle","hideToggle",re],togglePosition:"togglePosition"},outputs:{afterExpand:"afterExpand",afterCollapse:"afterCollapse"},exportAs:["matExpansionPanel"],features:[Ke([{provide:Gp,useValue:void 0},{provide:ID,useExisting:n}]),Pe,it],ngContentSelectors:IR,decls:9,vars:4,consts:[["bodyWrapper",""],["body",""],[1,"mat-expansion-panel-content-wrapper"],["role","region",1,"mat-expansion-panel-content",3,"id"],[1,"mat-expansion-panel-body"],[3,"cdkPortalOutlet"]],template:function(r,i){r&1&&(Ze(kR),ae(0),I(1,"div",2,0)(3,"div",3,1)(5,"div",4),ae(6,1),Ue(7,SR,0,0,"ng-template",5),T(),ae(8,2),T()()),r&2&&(S(),ue("inert",i.expanded?null:""),S(2),te("id",i.id),ue("aria-labelledby",i._headerId),S(4),te("cdkPortalOutlet",i._portal))},dependencies:[Un],styles:[`.mat-expansion-panel {
  box-sizing: content-box;
  display: block;
  margin: 0;
  overflow: hidden;
}
.mat-expansion-panel.mat-expansion-panel-animations-enabled {
  transition: margin 225ms cubic-bezier(0.4, 0, 0.2, 1), box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel {
  position: relative;
  background: var(--mat-expansion-container-background-color, var(--mat-sys-surface));
  color: var(--mat-expansion-container-text-color, var(--mat-sys-on-surface));
  border-radius: var(--mat-expansion-container-shape, 12px);
}
.mat-expansion-panel:not([class*=mat-elevation-z]) {
  box-shadow: var(--mat-expansion-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}
.mat-accordion .mat-expansion-panel:not(.mat-expanded), .mat-accordion .mat-expansion-panel:not(.mat-expansion-panel-spacing) {
  border-radius: 0;
}
.mat-accordion .mat-expansion-panel:first-of-type {
  border-top-right-radius: var(--mat-expansion-container-shape, 12px);
  border-top-left-radius: var(--mat-expansion-container-shape, 12px);
}
.mat-accordion .mat-expansion-panel:last-of-type {
  border-bottom-right-radius: var(--mat-expansion-container-shape, 12px);
  border-bottom-left-radius: var(--mat-expansion-container-shape, 12px);
}
@media (forced-colors: active) {
  .mat-expansion-panel {
    outline: solid 1px;
  }
}

.mat-expansion-panel-content-wrapper {
  display: grid;
  grid-template-rows: 0fr;
  grid-template-columns: 100%;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-content-wrapper {
  transition: grid-template-rows 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
  grid-template-rows: 1fr;
}
@supports not (grid-template-rows: 0fr) {
  .mat-expansion-panel-content-wrapper {
    height: 0;
  }
  .mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
    height: auto;
  }
}
@media print {
  .mat-expansion-panel-content-wrapper {
    height: 0;
  }
  .mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
    height: auto;
  }
}

.mat-expansion-panel-content {
  display: flex;
  flex-direction: column;
  overflow: visible;
  min-height: 0;
  visibility: hidden;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-content {
  transition: visibility 190ms linear;
}
.mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper > .mat-expansion-panel-content {
  visibility: visible;
}
.mat-expansion-panel-content {
  font-family: var(--mat-expansion-container-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-expansion-container-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-expansion-container-text-weight, var(--mat-sys-body-large-weight));
  line-height: var(--mat-expansion-container-text-line-height, var(--mat-sys-body-large-line-height));
  letter-spacing: var(--mat-expansion-container-text-tracking, var(--mat-sys-body-large-tracking));
}

.mat-expansion-panel-body {
  padding: 0 24px 16px;
}

.mat-expansion-panel-spacing {
  margin: 16px 0;
}
.mat-accordion > .mat-expansion-panel-spacing:first-child, .mat-accordion > *:first-child:not(.mat-expansion-panel) .mat-expansion-panel-spacing {
  margin-top: 0;
}
.mat-accordion > .mat-expansion-panel-spacing:last-child, .mat-accordion > *:last-child:not(.mat-expansion-panel) .mat-expansion-panel-spacing {
  margin-bottom: 0;
}

.mat-action-row {
  border-top-style: solid;
  border-top-width: 1px;
  display: flex;
  flex-direction: row;
  justify-content: flex-end;
  padding: 16px 8px 16px 24px;
  border-top-color: var(--mat-expansion-actions-divider-color, var(--mat-sys-outline));
}
.mat-action-row .mat-button-base,
.mat-action-row .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-action-row .mat-button-base,
[dir=rtl] .mat-action-row .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}
`],encapsulation:2,changeDetection:0})}return n})();var qp=(()=>{class n{panel=f(Wp,{host:!0});_element=f(z);_focusMonitor=f(Pi);_changeDetectorRef=f(tt);_parentChangeSubscription=ye.EMPTY;constructor(){f(vt).load(Vr);let e=this.panel,r=f(SD,{optional:!0}),i=f(new Fr("tabindex"),{optional:!0}),o=e.accordion?e.accordion._stateChanges.pipe(Ie(s=>!!(s.hideToggle||s.togglePosition))):Xr;this.tabIndex=parseInt(i||"")||0,this._parentChangeSubscription=an(e.opened,e.closed,o,e._inputChanges.pipe(Ie(s=>!!(s.hideToggle||s.disabled||s.togglePosition)))).subscribe(()=>this._changeDetectorRef.markForCheck()),e.closed.pipe(Ie(()=>e._containsFocus())).subscribe(()=>this._focusMonitor.focusVia(this._element,"program")),r&&(this.expandedHeight=r.expandedHeight,this.collapsedHeight=r.collapsedHeight)}expandedHeight;collapsedHeight;tabIndex=0;get disabled(){return this.panel.disabled}_toggle(){this.disabled||this.panel.toggle()}_isExpanded(){return this.panel.expanded}_getExpandedState(){return this.panel._getExpandedState()}_getPanelId(){return this.panel.id}_getTogglePosition(){return this.panel.togglePosition}_showToggle(){return!this.panel.hideToggle&&!this.panel.disabled}_getHeaderHeight(){let e=this._isExpanded();return e&&this.expandedHeight?this.expandedHeight:!e&&this.collapsedHeight?this.collapsedHeight:null}_keydown(e){switch(e.keyCode){case 32:case 13:xt(e)||(e.preventDefault(),this._toggle());break;default:this.panel.accordion&&this.panel.accordion._handleHeaderKeydown(e);return}}focus(e,r){e?this._focusMonitor.focusVia(this._element,e,r):this._element.nativeElement.focus(r)}ngAfterViewInit(){this._focusMonitor.monitor(this._element).subscribe(e=>{e&&this.panel.accordion&&this.panel.accordion._handleHeaderFocus(this)})}ngOnDestroy(){this._parentChangeSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._element)}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-expansion-panel-header"]],hostAttrs:["role","button",1,"mat-expansion-panel-header","mat-focus-indicator"],hostVars:13,hostBindings:function(r,i){r&1&&Ce("click",function(){return i._toggle()})("keydown",function(s){return i._keydown(s)}),r&2&&(ue("id",i.panel._headerId)("tabindex",i.disabled?-1:i.tabIndex)("aria-controls",i._getPanelId())("aria-expanded",i._isExpanded())("aria-disabled",i.panel.disabled),cr("height",i._getHeaderHeight()),Q("mat-expanded",i._isExpanded())("mat-expansion-toggle-indicator-after",i._getTogglePosition()==="after")("mat-expansion-toggle-indicator-before",i._getTogglePosition()==="before"))},inputs:{expandedHeight:"expandedHeight",collapsedHeight:"collapsedHeight",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:jt(e)]},ngContentSelectors:MR,decls:5,vars:3,consts:[[1,"mat-content"],[1,"mat-expansion-indicator"],["xmlns","http://www.w3.org/2000/svg","viewBox","0 -960 960 960","aria-hidden","true","focusable","false"],["d","M480-345 240-585l56-56 184 184 184-184 56 56-240 240Z"]],template:function(r,i){r&1&&(Ze(TR),Bt(0,"span",0),ae(1),ae(2,1),ae(3,2),Xt(),pe(4,AR,3,0,"span",1)),r&2&&(Q("mat-content-hide-toggle",!i._showToggle()),S(4),ge(i._showToggle()?4:-1))},styles:[`.mat-expansion-panel-header {
  display: flex;
  flex-direction: row;
  align-items: center;
  padding: 0 24px;
  border-radius: inherit;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-header {
  transition: height 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel-header::before {
  border-radius: inherit;
}
.mat-expansion-panel-header {
  height: var(--mat-expansion-header-collapsed-state-height, 48px);
  font-family: var(--mat-expansion-header-text-font, var(--mat-sys-title-medium-font));
  font-size: var(--mat-expansion-header-text-size, var(--mat-sys-title-medium-size));
  font-weight: var(--mat-expansion-header-text-weight, var(--mat-sys-title-medium-weight));
  line-height: var(--mat-expansion-header-text-line-height, var(--mat-sys-title-medium-line-height));
  letter-spacing: var(--mat-expansion-header-text-tracking, var(--mat-sys-title-medium-tracking));
}
.mat-expansion-panel-header.mat-expanded {
  height: var(--mat-expansion-header-expanded-state-height, 64px);
}
.mat-expansion-panel-header[aria-disabled=true] {
  color: var(--mat-expansion-header-disabled-state-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-expansion-panel-header:not([aria-disabled=true]) {
  cursor: pointer;
}
.mat-expansion-panel:not(.mat-expanded) .mat-expansion-panel-header:not([aria-disabled=true]):hover {
  background: var(--mat-expansion-header-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
@media (hover: none) {
  .mat-expansion-panel:not(.mat-expanded) .mat-expansion-panel-header:not([aria-disabled=true]):hover {
    background: var(--mat-expansion-container-background-color, var(--mat-sys-surface));
  }
}
.mat-expansion-panel .mat-expansion-panel-header:not([aria-disabled=true]).cdk-keyboard-focused, .mat-expansion-panel .mat-expansion-panel-header:not([aria-disabled=true]).cdk-program-focused {
  background: var(--mat-expansion-header-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
.mat-expansion-panel-header._mat-animation-noopable {
  transition: none;
}
.mat-expansion-panel-header:focus, .mat-expansion-panel-header:hover {
  outline: none;
}
.mat-expansion-panel-header.mat-expanded:focus, .mat-expansion-panel-header.mat-expanded:hover {
  background: inherit;
}
.mat-expansion-panel-header.mat-expansion-toggle-indicator-before {
  flex-direction: row-reverse;
}
.mat-expansion-panel-header.mat-expansion-toggle-indicator-before .mat-expansion-indicator {
  margin: 0 16px 0 0;
}
[dir=rtl] .mat-expansion-panel-header.mat-expansion-toggle-indicator-before .mat-expansion-indicator {
  margin: 0 0 0 16px;
}

.mat-content {
  display: flex;
  flex: 1;
  flex-direction: row;
  overflow: hidden;
}
.mat-content.mat-content-hide-toggle {
  margin-right: 8px;
}
[dir=rtl] .mat-content.mat-content-hide-toggle {
  margin-right: 0;
  margin-left: 8px;
}
.mat-expansion-toggle-indicator-before .mat-content.mat-content-hide-toggle {
  margin-left: 24px;
  margin-right: 0;
}
[dir=rtl] .mat-expansion-toggle-indicator-before .mat-content.mat-content-hide-toggle {
  margin-right: 24px;
  margin-left: 0;
}

.mat-expansion-panel-header-title {
  color: var(--mat-expansion-header-text-color, var(--mat-sys-on-surface));
}

.mat-expansion-panel-header-title,
.mat-expansion-panel-header-description {
  display: flex;
  flex-grow: 1;
  flex-basis: 0;
  margin-right: 16px;
  align-items: center;
}
[dir=rtl] .mat-expansion-panel-header-title,
[dir=rtl] .mat-expansion-panel-header-description {
  margin-right: 0;
  margin-left: 16px;
}
.mat-expansion-panel-header[aria-disabled=true] .mat-expansion-panel-header-title,
.mat-expansion-panel-header[aria-disabled=true] .mat-expansion-panel-header-description {
  color: inherit;
}

.mat-expansion-panel-header-description {
  flex-grow: 2;
  color: var(--mat-expansion-header-description-color, var(--mat-sys-on-surface-variant));
}

.mat-expansion-panel-animations-enabled .mat-expansion-indicator {
  transition: transform 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel-header.mat-expanded .mat-expansion-indicator {
  transform: rotate(180deg);
}
.mat-expansion-indicator::after {
  border-style: solid;
  border-width: 0 2px 2px 0;
  content: "";
  padding: 3px;
  transform: rotate(45deg);
  vertical-align: middle;
  color: var(--mat-expansion-header-indicator-color, var(--mat-sys-on-surface-variant));
  display: var(--mat-expansion-legacy-header-indicator-display, none);
}
.mat-expansion-indicator svg {
  width: 24px;
  height: 24px;
  margin: 0 -8px;
  vertical-align: middle;
  fill: var(--mat-expansion-header-indicator-color, var(--mat-sys-on-surface-variant));
  display: var(--mat-expansion-header-indicator-display, inline-block);
}

@media (forced-colors: active) {
  .mat-expansion-panel-content {
    border-top: 1px solid;
    border-top-left-radius: 0;
    border-top-right-radius: 0;
  }
}
`],encapsulation:2,changeDetection:0})}return n})();var TD=(()=>{class n{static \u0275fac=function(r){return new(r||n)};static \u0275dir=Y({type:n,selectors:[["mat-panel-title"]],hostAttrs:[1,"mat-expansion-panel-header-title"]})}return n})(),MD=(()=>{class n extends ED{_keyManager;_ownHeaders=new nr;_headers;hideToggle=!1;displayMode="default";togglePosition="after";ngAfterContentInit(){this._headers.changes.pipe(pt(this._headers)).subscribe(e=>{this._ownHeaders.reset(e.filter(r=>r.panel.accordion===this)),this._ownHeaders.notifyOnChanges()}),this._keyManager=new ea(this._ownHeaders).withWrap().withHomeAndEnd()}_handleHeaderKeydown(e){this._keyManager.onKeydown(e)}_handleHeaderFocus(e){this._keyManager.updateActiveItem(e)}ngOnDestroy(){super.ngOnDestroy(),this._keyManager?.destroy(),this._ownHeaders.destroy()}static \u0275fac=(()=>{let e;return function(i){return(e||(e=Kt(n)))(i||n)}})();static \u0275dir=Y({type:n,selectors:[["mat-accordion"]],contentQueries:function(r,i,o){if(r&1&&_n(o,qp,5),r&2){let s;J(s=ee())&&(i._headers=s)}},hostAttrs:[1,"mat-accordion"],hostVars:2,hostBindings:function(r,i){r&2&&Q("mat-accordion-multi",i.multi)},inputs:{hideToggle:[2,"hideToggle","hideToggle",re],displayMode:"displayMode",togglePosition:"togglePosition"},exportAs:["matAccordion"],features:[Ke([{provide:Gp,useExisting:n}]),Pe]})}return n})();var OR=["mat-internal-form-field",""],NR=["*"],AD=(()=>{class n{labelPosition="after";static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(r,i){r&2&&Q("mdc-form-field--align-end",i.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:OR,ngContentSelectors:NR,decls:1,vars:0,template:function(r,i){r&1&&(Ze(),ae(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2,changeDetection:0})}return n})();var PR=["input"],FR=["label"],LR=["*"],Yp={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},BR=new x("mat-checkbox-default-options",{providedIn:"root",factory:()=>Yp}),wt=(function(n){return n[n.Init=0]="Init",n[n.Checked=1]="Checked",n[n.Unchecked=2]="Unchecked",n[n.Indeterminate=3]="Indeterminate",n})(wt||{}),Zp=class{source;checked},RD=(()=>{class n{_elementRef=f(z);_changeDetectorRef=f(tt);_ngZone=f(F);_animationsDisabled=Ge();_options=f(BR,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let r=new Zp;return r.source=this,r.checked=e,r}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new j;indeterminateChange=new j;value;disableRipple=!1;_inputElement;_labelElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=wt.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){f(vt).load(Vr);let e=f(new Fr("tabindex"),{optional:!0});this._options=this._options||Yp,this.color=this._options.color||Yp.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=f(Fe).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let r=e!=this._indeterminate();this._indeterminate.set(e),r&&(e?this._transitionCheckState(wt.Indeterminate):this._transitionCheckState(this.checked?wt.Checked:wt.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=X(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let r=this._currentCheckState,i=this._getAnimationTargetElement();if(!(r===e||!i)&&(this._currentAnimationClass&&i.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(r,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){i.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{i.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?wt.Checked:wt.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,r){if(this._animationsDisabled)return"";switch(e){case wt.Init:if(r===wt.Checked)return this._animationClasses.uncheckedToChecked;if(r==wt.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case wt.Unchecked:return r===wt.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case wt.Checked:return r===wt.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case wt.Indeterminate:return r===wt.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let r=this._inputElement;r&&(r.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_onTouchTargetClick(){this._handleInputClick(),this.disabled||this._inputElement.nativeElement.focus()}_preventBubblingFromLabel(e){e.target&&this._labelElement.nativeElement.contains(e.target)&&e.stopPropagation()}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["mat-checkbox"]],viewQuery:function(r,i){if(r&1&&et(PR,5)(FR,5),r&2){let o;J(o=ee())&&(i._inputElement=o.first),J(o=ee())&&(i._labelElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(r,i){r&2&&(Hn("id",i.id),ue("tabindex",null)("aria-label",null)("aria-labelledby",null),vn(i.color?"mat-"+i.color:"mat-accent"),Q("_mat-animation-noopable",i._animationsDisabled)("mdc-checkbox--disabled",i.disabled)("mat-mdc-checkbox-disabled",i.disabled)("mat-mdc-checkbox-checked",i.checked)("mat-mdc-checkbox-disabled-interactive",i.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",re],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",re],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",re],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:jt(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",re],checked:[2,"checked","checked",re],disabled:[2,"disabled","disabled",re],indeterminate:[2,"indeterminate","indeterminate",re]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[Ke([{provide:Cd,useExisting:Ut(()=>n),multi:!0},{provide:ga,useExisting:n,multi:!0}]),it],ngContentSelectors:LR,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target",3,"click"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-label",3,"for"]],template:function(r,i){if(r&1&&(Ze(),I(0,"div",3),Ce("click",function(s){return i._preventBubblingFromLabel(s)}),I(1,"div",4,0)(3,"div",5),Ce("click",function(){return i._onTouchTargetClick()}),T(),I(4,"input",6,1),Ce("blur",function(){return i._onBlur()})("click",function(){return i._onInputClick()})("change",function(s){return i._onInteractionEvent(s)}),T(),fe(6,"div",7),I(7,"div",8),mn(),I(8,"svg",9),fe(9,"path",10),T(),go(),fe(10,"div",11),T(),fe(11,"div",12),T(),I(12,"label",13,2),ae(14),T()()),r&2){let o=Vt(2);te("labelPosition",i.labelPosition),S(4),Q("mdc-checkbox--selected",i.checked),te("checked",i.checked)("indeterminate",i.indeterminate)("disabled",i.disabled&&!i.disabledInteractive)("id",i.inputId)("required",i.required)("tabIndex",i.disabled&&!i.disabledInteractive?-1:i.tabIndex),ue("aria-label",i.ariaLabel||null)("aria-labelledby",i.ariaLabelledby)("aria-describedby",i.ariaDescribedby)("aria-checked",i.indeterminate?"mixed":null)("aria-controls",i.ariaControls)("aria-disabled",i.disabled&&i.disabledInteractive?!0:null)("aria-expanded",i.ariaExpanded)("aria-owns",i.ariaOwns)("name",i.name)("value",i.value),S(7),te("matRippleTrigger",o)("matRippleDisabled",i.disableRipple||i.disabled)("matRippleCentered",!0),S(),te("for",i.inputId)}},dependencies:[dd,AD],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-checkbox-state-layer-size, 40px);
  height: var(--mat-checkbox-state-layer-size, 40px);
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--mat-checkbox-unselected-icon-color, var(--mat-sys-on-surface-variant));
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-hover-icon-color, var(--mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-focus-icon-color, var(--mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--mat-checkbox-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-checkbox-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-checkbox-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-checkbox-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-checkbox-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-checkbox-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
  color: var(--mat-checkbox-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
    color: GrayText;
  }
}
.mat-mdc-checkbox label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-checkbox-touch-target-size, 48px);
  width: var(--mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return n})();function Gi(n,t,e,r,i){if(n===t)return!e.strictZero||n!==0||1/n===1/t;if(typeof n=="number"&&typeof t=="number"&&isNaN(n)&&isNaN(t))return e.nanEqual;if((n===null||typeof n!="object"&&typeof n!="function")&&(t===null||typeof t!="object"&&typeof t!="function")||i>e.maxDepth||!n||!t||typeof n!="object"||typeof t!="object"||n.constructor!==t.constructor&&e.checkPrototypes)return!1;if(r.has(n))return r.get(n)===t;if(r.set(n,t),Array.isArray(n)){let o=n,s=t,a=o.length;if(a!==s.length)return!1;for(let c=a;c--!==0;)if(!Gi(o[c],s[c],e,r,i+1))return!1;return!0}if(n.constructor===RegExp){let o=n,s=t;return o.source===s.source&&o.flags===s.flags}if(n.constructor===Date){let o=n,s=t;return o.getTime()===s.getTime()}if(n.constructor===ArrayBuffer){let o=n,s=t;if(o.byteLength!==s.byteLength)return!1;let a=new Uint8Array(o),c=new Uint8Array(s);for(let l=0;l<a.length;l++)if(a[l]!==c[l])return!1;return!0}if(ArrayBuffer.isView(n)){let o=n,s=t;if(o.constructor!==s.constructor||o.length!==s.length)return!1;for(let a=0;a<o.length;a++){let c=o[a],l=s[a];if(c!==l&&!(e.nanEqual&&typeof c=="number"&&typeof l=="number"&&Number.isNaN(c)&&Number.isNaN(l)))return!1}return!0}if(n.valueOf!==Object.prototype.valueOf)return n.valueOf()===t.valueOf();if(n.constructor===Error){let o=n,s=t;return o.name===s.name&&o.message===s.message}if(n.constructor===Set){let o=n,s=t;return o.size!==s.size?!1:VR(o,s,e,r,i)}if(n.constructor===Map){let o=n,s=t;return o.size!==s.size?!1:jR(o,s,e,r,i)}return HR(n,t,e,r,i)}function VR(n,t,e,r,i){if(n.size<=10){let c=new Set;for(let l of n){let d=!1,u=0;for(let m of t){if(!c.has(u)&&Gi(l,m,e,new WeakMap,i+1)){c.add(u),d=!0;break}u++}if(!d)return!1}return!0}let o=Array.from(n),s=Array.from(t);if(o.length!==s.length)return!1;let a=new Set;for(let c=o.length;c--!==0;){let l=!1;for(let d=s.length;d--!==0;)if(!a.has(d)){let u=new WeakMap;if(Gi(o[c],s[d],e,u,i+1)){a.add(d),l=!0;break}}if(!l)return!1}return!0}function jR(n,t,e,r,i){for(let[o,s]of n){let a=!1;for(let[c,l]of t)if(Gi(o,c,e,r,i+1)&&Gi(s,l,e,r,i+1)){a=!0;break}if(!a)return!1}return!0}function HR(n,t,e,r,i){let o=Object.keys(n),s=o.length;if(s!==Object.keys(t).length)return!1;for(let a=s;a--!==0;)if(!Object.prototype.hasOwnProperty.call(t,o[a]))return!1;for(let a=s;a--!==0;){let c=o[a];if(!Gi(n[c],t[c],e,r,i+1))return!1}return!0}function zR(n,t,e={}){let r={nanEqual:e.nanEqual??!0,checkPrototypes:e.checkPrototypes??!1,strictZero:e.strictZero??!1,maxDepth:e.maxDepth??1e3};return Gi(n,t,r,new WeakMap,0)}var Ld=zR;var UR=(n,t)=>t.id;function $R(n,t){if(n&1&&fe(0,"h1",5),n&2){let e=ne(2).$implicit;te("innerHTML",e.plainText,Ei)}}function GR(n,t){if(n&1&&fe(0,"h2",5),n&2){let e=ne(2).$implicit;te("innerHTML",e.plainText,Ei)}}function WR(n,t){if(n&1&&(pe(0,$R,1,1,"h1",5),pe(1,GR,1,1,"h2",5)),n&2){let e=ne().$implicit;ge(e.level===1?0:-1),S(),ge(e.level===2?1:-1)}}function qR(n,t){n&1&&fe(0,"mat-divider")}function YR(n,t){if(n&1&&fe(0,"p",2),n&2){let e=ne().$implicit;te("innerHTML",e.html,Ei)}}function ZR(n,t){if(n&1&&fe(0,"div",3),n&2){let e=ne().$implicit;te("innerHTML",e.html,Ei)}}function KR(n,t){if(n&1&&fe(0,"h3",5),n&2){let e=t.$implicit;te("innerHTML",e,Ei)}}function QR(n,t){if(n&1){let e=jn();I(0,"mat-expansion-panel",7),Ce("expandedChange",function(i){let o=Yt(e).$implicit,s=ne(3);return Zt(s.markArrowExpanded(o.id,i))}),I(1,"mat-expansion-panel-header")(2,"mat-panel-title",8)(3,"mat-checkbox",9),Ce("click",function(i){return i.stopPropagation()})("keydown",function(i){return i.stopPropagation()})("change",function(i){let o=Yt(e).$implicit,s=ne(3);return Zt(s.markArrowCompleted(o.id,i.checked))}),T(),_t(4),T()(),I(5,"div",10),Ii(6,KR,1,1,"h3",5,xl),T()()}if(n&2){let e=t.$implicit,r=ne(3);te("expanded",r.expandedNodes().has(e.id)),S(3),te("checked",r.completedArrows().has(e.id)),S(),en(" ",e.text," "),S(2),Si(e.spoilers)}}function XR(n,t){if(n&1&&(I(0,"mat-accordion",4),Ii(1,QR,8,3,"mat-expansion-panel",6,UR),T()),n&2){let e=ne().$implicit;S(),Si(e.items)}}function JR(n,t){if(n&1&&pe(0,WR,2,2)(1,qR,1,0,"mat-divider")(2,YR,1,1,"p",2)(3,ZR,1,1,"div",3)(4,XR,3,0,"mat-accordion",4),n&2){let e,r=t.$implicit;ge((e=r.type)==="heading"?0:e==="thematic-break"?1:e==="paragraph"?2:e==="inline-html"?3:e==="list"?4:-1)}}var Bd=class n{bossfightDocumentProcessor=f(Fd);userDataService=f($o);userData=this.userDataService.userData;bossfightDocumentRecord=Nv.required({alias:"documentRecord"});bossfightMarkdown=yh({params:()=>({documentUrl:this.bossfightDocumentRecord().path}),loader:({params:{documentUrl:t}})=>fetch(t).then(e=>e.text()),defaultValue:"# Loading..."});blocks=Me(()=>this.bossfightDocumentProcessor.parseMarkdownDocument(this.bossfightMarkdown.value()));expandedNodes=St({source:this.userData,computation:t=>{let i=((t.documentStates??{})[this.bossfightDocumentRecord().id]??{}).readStates??{},o=new Set;return Object.entries(i).forEach(([s,{spoilerRevealed:a}])=>{a&&o.add(s)}),o}});completedArrows=St({source:this.userData,computation:t=>{let i=((t.documentStates??{})[this.bossfightDocumentRecord().id]??{}).readStates??{},o=new Set;return Object.entries(i).forEach(([s,{arrowCompleted:a}])=>{a&&o.add(s)}),o}});updateUserData=bt(()=>{let t=this.bossfightDocumentRecord().id,e=structuredClone(this.userData()),r={},i=this.expandedNodes().intersection(this.completedArrows()),o=this.completedArrows().difference(this.expandedNodes()),s=this.expandedNodes().difference(this.completedArrows());i.forEach(a=>{r[a]={arrowCompleted:!0,spoilerRevealed:!0}}),o.forEach(a=>{r[a]={arrowCompleted:!0,spoilerRevealed:!1}}),s.forEach(a=>{r[a]={arrowCompleted:!1,spoilerRevealed:!0}}),e.documentStates??={},e.documentStates[t]??={},e.documentStates[t].readStates??={},!Ld(e.documentStates[t].readStates,r)&&(e.documentStates[t].readStates=r,this.userDataService.saveUserData(e))});markArrowExpanded(t,e){this.expandedNodes.update(r=>{let i=new Set(r);return e?i.add(t):i.delete(t),i})}markArrowCompleted(t,e){this.completedArrows.update(r=>{let i=new Set(r);return e?i.add(t):i.delete(t),i})}static \u0275fac=function(e){return new(e||n)};static \u0275cmp=q({type:n,selectors:[["bossfight-render-component"]],inputs:{bossfightDocumentRecord:[1,"documentRecord","bossfightDocumentRecord"]},decls:4,vars:0,consts:[[1,"layout"],[1,"content"],[1,"mat-body-1",3,"innerHTML"],[1,"inline-html",3,"innerHTML"],["multi",""],[3,"innerHTML"],[1,"item-panel",3,"expanded"],[1,"item-panel",3,"expandedChange","expanded"],[1,"item-panel-title"],[3,"click","keydown","change","checked"],[1,"panel-content"]],template:function(e,r){e&1&&(I(0,"div",0)(1,"div",1),Ii(2,JR,5,1,null,null,xl),T()()),e&2&&(S(2),Si(r.blocks()))},dependencies:[Up,MD,Wp,qp,TD,RD],styles:[".content[_ngcontent-%COMP%]{display:grid;grid-template-columns:minmax(600px,1fr);justify-content:center}.content[_ngcontent-%COMP%]   .inline-html[_ngcontent-%COMP%]{width:fit-content;justify-self:center}.content[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%], .content[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%], .content[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%], .content[_ngcontent-%COMP%]   h4[_ngcontent-%COMP%], .content[_ngcontent-%COMP%]   h5[_ngcontent-%COMP%]{text-align:center}.content[_ngcontent-%COMP%]   .item-panel-title[_ngcontent-%COMP%]{gap:8px}.content[_ngcontent-%COMP%]   .panel-content[_ngcontent-%COMP%]{background-color:var(--mat-sys-surface-container);color:var(--mat-sys-on-surface-variant);border-radius:var(--mat-sys-corner-small)}.content[_ngcontent-%COMP%]   .panel-content[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{padding-block:1em;margin-block:0}.content[_ngcontent-%COMP%]   .panel-content[_ngcontent-%COMP%]   h5[_ngcontent-%COMP%]{padding-block:1.67em;margin-block:0}"]})};function Ct(n){return n<0?-1:n===0?0:1}function Wi(n,t,e){return(1-e)*n+e*t}function OD(n,t,e){return e<n?n:e>t?t:e}function nt(n,t,e){return e<n?n:e>t?t:e}function Vd(n){return n=n%360,n<0&&(n=n+360),n}function dt(n){return n=n%360,n<0&&(n=n+360),n}function va(n,t){let e=n[0]*t[0][0]+n[1]*t[0][1]+n[2]*t[0][2],r=n[0]*t[1][0]+n[1]*t[1][1]+n[2]*t[1][2],i=n[0]*t[2][0]+n[1]*t[2][1]+n[2]*t[2][2];return[e,r,i]}var ND=[[.41233895,.35762064,.18051042],[.2126,.7152,.0722],[.01932141,.11916382,.95034478]],eO=[[3.2413774792388685,-1.5376652402851851,-.49885366846268053],[-.9691452513005321,1.8758853451067872,.04156585616912061],[.05562093689691305,-.20395524564742123,1.0571799111220335]],PD=[95.047,100,108.883];function jd(n,t,e){return(255<<24|(n&255)<<16|(t&255)<<8|e&255)>>>0}function Kp(n){let t=qi(n[0]),e=qi(n[1]),r=qi(n[2]);return jd(t,e,r)}function Da(n){return n>>16&255}function wa(n){return n>>8&255}function Ca(n){return n&255}function FD(n,t,e){let r=eO,i=r[0][0]*n+r[0][1]*t+r[0][2]*e,o=r[1][0]*n+r[1][1]*t+r[1][2]*e,s=r[2][0]*n+r[2][1]*t+r[2][2]*e,a=qi(i),c=qi(o),l=qi(s);return jd(a,c,l)}function tO(n){let t=Gn(Da(n)),e=Gn(wa(n)),r=Gn(Ca(n));return va([t,e,r],ND)}function Qp(n){let t=Gn(Da(n)),e=Gn(wa(n)),r=Gn(Ca(n)),i=ND,o=i[0][0]*t+i[0][1]*e+i[0][2]*r,s=i[1][0]*t+i[1][1]*e+i[1][2]*r,a=i[2][0]*t+i[2][1]*e+i[2][2]*r,c=PD,l=o/c[0],d=s/c[1],u=a/c[2],m=xa(l),h=xa(d),p=xa(u),g=116*h-16,y=500*(m-h),D=200*(h-p);return[g,y,D]}function LD(n){let t=In(n),e=qi(t);return jd(e,e,e)}function Hd(n){let t=tO(n)[1];return 116*xa(t/100)-16}function In(n){return 100*nO((n+16)/116)}function Ea(n){return xa(n/100)*116-16}function Gn(n){let t=n/255;return t<=.040449936?t/12.92*100:Math.pow((t+.055)/1.055,2.4)*100}function qi(n){let t=n/100,e=0;return t<=.0031308?e=t*12.92:e=1.055*Math.pow(t,1/2.4)-.055,OD(0,255,Math.round(e*255))}function BD(){return PD}function xa(n){let t=.008856451679035631,e=24389/27;return n>t?Math.pow(n,1/3):(e*n+16)/116}function nO(n){let t=.008856451679035631,e=24389/27,r=n*n*n;return r>t?r:(116*n-16)/e}var Ht=class n{static make(t=BD(),e=200/Math.PI*In(50)/100,r=50,i=2,o=!1){let s=t,a=s[0]*.401288+s[1]*.650173+s[2]*-.051461,c=s[0]*-.250268+s[1]*1.204414+s[2]*.045854,l=s[0]*-.002079+s[1]*.048952+s[2]*.953127,d=.8+i/10,u=d>=.9?Wi(.59,.69,(d-.9)*10):Wi(.525,.59,(d-.8)*10),m=o?1:d*(1-1/3.6*Math.exp((-e-42)/92));m=m>1?1:m<0?0:m;let h=d,p=[m*(100/a)+1-m,m*(100/c)+1-m,m*(100/l)+1-m],g=1/(5*e+1),y=g*g*g*g,D=1-y,R=y*e+.1*D*D*Math.cbrt(5*e),oe=In(r)/t[1],Z=1.48+Math.sqrt(oe),we=.725/Math.pow(oe,.2),me=we,de=[Math.pow(R*p[0]*a/100,.42),Math.pow(R*p[1]*c/100,.42),Math.pow(R*p[2]*l/100,.42)],Je=[400*de[0]/(de[0]+27.13),400*de[1]/(de[1]+27.13),400*de[2]/(de[2]+27.13)],ut=(2*Je[0]+Je[1]+.05*Je[2])*we;return new n(oe,ut,we,me,u,h,p,R,Math.pow(R,.25),Z)}constructor(t,e,r,i,o,s,a,c,l,d){this.n=t,this.aw=e,this.nbb=r,this.ncb=i,this.c=o,this.nc=s,this.rgbD=a,this.fl=c,this.fLRoot=l,this.z=d}};Ht.DEFAULT=Ht.make();var Wn=class n{constructor(t,e,r,i,o,s,a,c,l){this.hue=t,this.chroma=e,this.j=r,this.q=i,this.m=o,this.s=s,this.jstar=a,this.astar=c,this.bstar=l}distance(t){let e=this.jstar-t.jstar,r=this.astar-t.astar,i=this.bstar-t.bstar,o=Math.sqrt(e*e+r*r+i*i);return 1.41*Math.pow(o,.63)}static fromInt(t){return n.fromIntInViewingConditions(t,Ht.DEFAULT)}static fromIntInViewingConditions(t,e){let r=(t&16711680)>>16,i=(t&65280)>>8,o=t&255,s=Gn(r),a=Gn(i),c=Gn(o),l=.41233895*s+.35762064*a+.18051042*c,d=.2126*s+.7152*a+.0722*c,u=.01932141*s+.11916382*a+.95034478*c,m=.401288*l+.650173*d-.051461*u,h=-.250268*l+1.204414*d+.045854*u,p=-.002079*l+.048952*d+.953127*u,g=e.rgbD[0]*m,y=e.rgbD[1]*h,D=e.rgbD[2]*p,R=Math.pow(e.fl*Math.abs(g)/100,.42),oe=Math.pow(e.fl*Math.abs(y)/100,.42),Z=Math.pow(e.fl*Math.abs(D)/100,.42),we=Ct(g)*400*R/(R+27.13),me=Ct(y)*400*oe/(oe+27.13),de=Ct(D)*400*Z/(Z+27.13),Je=(11*we+-12*me+de)/11,ut=(we+me-2*de)/9,Tt=(20*we+20*me+21*de)/20,ot=(40*we+20*me+de)/20,Zi=Math.atan2(ut,Je)*180/Math.PI,hr=dt(Zi),qn=hr*Math.PI/180,Ta=ot*e.nbb,pr=100*Math.pow(Ta/e.aw,e.c*e.z),Ma=4/e.c*Math.sqrt(pr/100)*(e.aw+4)*e.fLRoot,Kd=hr<20.14?hr+360:hr,Qd=.25*(Math.cos(Kd*Math.PI/180+2)+3.8),Jd=5e4/13*Qd*e.nc*e.ncb*Math.sqrt(Je*Je+ut*ut)/(Tt+.305),Aa=Math.pow(Jd,.9)*Math.pow(1.64-Math.pow(.29,e.n),.73),sg=Aa*Math.sqrt(pr/100),ag=sg*e.fLRoot,QD=50*Math.sqrt(Aa*e.c/(e.aw+4)),XD=(1+100*.007)*pr/(1+.007*pr),cg=1/.0228*Math.log(1+.0228*ag),JD=cg*Math.cos(qn),ew=cg*Math.sin(qn);return new n(hr,sg,pr,Ma,ag,QD,XD,JD,ew)}static fromJch(t,e,r){return n.fromJchInViewingConditions(t,e,r,Ht.DEFAULT)}static fromJchInViewingConditions(t,e,r,i){let o=4/i.c*Math.sqrt(t/100)*(i.aw+4)*i.fLRoot,s=e*i.fLRoot,a=e/Math.sqrt(t/100),c=50*Math.sqrt(a*i.c/(i.aw+4)),l=r*Math.PI/180,d=(1+100*.007)*t/(1+.007*t),u=1/.0228*Math.log(1+.0228*s),m=u*Math.cos(l),h=u*Math.sin(l);return new n(r,e,t,o,s,c,d,m,h)}static fromUcs(t,e,r){return n.fromUcsInViewingConditions(t,e,r,Ht.DEFAULT)}static fromUcsInViewingConditions(t,e,r,i){let o=e,s=r,a=Math.sqrt(o*o+s*s),l=(Math.exp(a*.0228)-1)/.0228/i.fLRoot,d=Math.atan2(s,o)*(180/Math.PI);d<0&&(d+=360);let u=t/(1-(t-100)*.007);return n.fromJchInViewingConditions(u,l,d,i)}toInt(){return this.viewed(Ht.DEFAULT)}viewed(t){let e=this.chroma===0||this.j===0?0:this.chroma/Math.sqrt(this.j/100),r=Math.pow(e/Math.pow(1.64-Math.pow(.29,t.n),.73),1/.9),i=this.hue*Math.PI/180,o=.25*(Math.cos(i+2)+3.8),s=t.aw*Math.pow(this.j/100,1/t.c/t.z),a=o*(5e4/13)*t.nc*t.ncb,c=s/t.nbb,l=Math.sin(i),d=Math.cos(i),u=23*(c+.305)*r/(23*a+11*r*d+108*r*l),m=u*d,h=u*l,p=(460*c+451*m+288*h)/1403,g=(460*c-891*m-261*h)/1403,y=(460*c-220*m-6300*h)/1403,D=Math.max(0,27.13*Math.abs(p)/(400-Math.abs(p))),R=Ct(p)*(100/t.fl)*Math.pow(D,1/.42),oe=Math.max(0,27.13*Math.abs(g)/(400-Math.abs(g))),Z=Ct(g)*(100/t.fl)*Math.pow(oe,1/.42),we=Math.max(0,27.13*Math.abs(y)/(400-Math.abs(y))),me=Ct(y)*(100/t.fl)*Math.pow(we,1/.42),de=R/t.rgbD[0],Je=Z/t.rgbD[1],ut=me/t.rgbD[2],Tt=1.86206786*de-1.01125463*Je+.14918677*ut,ot=.38752654*de+.62144744*Je-.00897398*ut,mr=-.0158415*de-.03412294*Je+1.04996444*ut;return FD(Tt,ot,mr)}static fromXyzInViewingConditions(t,e,r,i){let o=.401288*t+.650173*e-.051461*r,s=-.250268*t+1.204414*e+.045854*r,a=-.002079*t+.048952*e+.953127*r,c=i.rgbD[0]*o,l=i.rgbD[1]*s,d=i.rgbD[2]*a,u=Math.pow(i.fl*Math.abs(c)/100,.42),m=Math.pow(i.fl*Math.abs(l)/100,.42),h=Math.pow(i.fl*Math.abs(d)/100,.42),p=Ct(c)*400*u/(u+27.13),g=Ct(l)*400*m/(m+27.13),y=Ct(d)*400*h/(h+27.13),D=(11*p+-12*g+y)/11,R=(p+g-2*y)/9,oe=(20*p+20*g+21*y)/20,Z=(40*p+20*g+y)/20,me=Math.atan2(R,D)*180/Math.PI,de=me<0?me+360:me>=360?me-360:me,Je=de*Math.PI/180,ut=Z*i.nbb,Tt=100*Math.pow(ut/i.aw,i.c*i.z),ot=4/i.c*Math.sqrt(Tt/100)*(i.aw+4)*i.fLRoot,mr=de<20.14?de+360:de,Zi=1/4*(Math.cos(mr*Math.PI/180+2)+3.8),qn=5e4/13*Zi*i.nc*i.ncb*Math.sqrt(D*D+R*R)/(oe+.305),Ta=Math.pow(qn,.9)*Math.pow(1.64-Math.pow(.29,i.n),.73),pr=Ta*Math.sqrt(Tt/100),Ma=pr*i.fLRoot,Kd=50*Math.sqrt(Ta*i.c/(i.aw+4)),Qd=(1+100*.007)*Tt/(1+.007*Tt),Xd=Math.log(1+.0228*Ma)/.0228,Jd=Xd*Math.cos(Je),Aa=Xd*Math.sin(Je);return new n(de,pr,Tt,ot,Ma,Kd,Qd,Jd,Aa)}xyzInViewingConditions(t){let e=this.chroma===0||this.j===0?0:this.chroma/Math.sqrt(this.j/100),r=Math.pow(e/Math.pow(1.64-Math.pow(.29,t.n),.73),1/.9),i=this.hue*Math.PI/180,o=.25*(Math.cos(i+2)+3.8),s=t.aw*Math.pow(this.j/100,1/t.c/t.z),a=o*(5e4/13)*t.nc*t.ncb,c=s/t.nbb,l=Math.sin(i),d=Math.cos(i),u=23*(c+.305)*r/(23*a+11*r*d+108*r*l),m=u*d,h=u*l,p=(460*c+451*m+288*h)/1403,g=(460*c-891*m-261*h)/1403,y=(460*c-220*m-6300*h)/1403,D=Math.max(0,27.13*Math.abs(p)/(400-Math.abs(p))),R=Ct(p)*(100/t.fl)*Math.pow(D,1/.42),oe=Math.max(0,27.13*Math.abs(g)/(400-Math.abs(g))),Z=Ct(g)*(100/t.fl)*Math.pow(oe,1/.42),we=Math.max(0,27.13*Math.abs(y)/(400-Math.abs(y))),me=Ct(y)*(100/t.fl)*Math.pow(we,1/.42),de=R/t.rgbD[0],Je=Z/t.rgbD[1],ut=me/t.rgbD[2],Tt=1.86206786*de-1.01125463*Je+.14918677*ut,ot=.38752654*de+.62144744*Je-.00897398*ut,mr=-.0158415*de-.03412294*Je+1.04996444*ut;return[Tt,ot,mr]}};var ka=(()=>{class n{static sanitizeRadians(e){return(e+Math.PI*8)%(Math.PI*2)}static trueDelinearized(e){let r=e/100,i=0;return r<=.0031308?i=r*12.92:i=1.055*Math.pow(r,1/2.4)-.055,i*255}static chromaticAdaptation(e){let r=Math.pow(Math.abs(e),.42);return Ct(e)*400*r/(r+27.13)}static hueOf(e){let r=va(e,n.SCALED_DISCOUNT_FROM_LINRGB),i=n.chromaticAdaptation(r[0]),o=n.chromaticAdaptation(r[1]),s=n.chromaticAdaptation(r[2]),a=(11*i+-12*o+s)/11,c=(i+o-2*s)/9;return Math.atan2(c,a)}static areInCyclicOrder(e,r,i){let o=n.sanitizeRadians(r-e),s=n.sanitizeRadians(i-e);return o<s}static intercept(e,r,i){return(r-e)/(i-e)}static lerpPoint(e,r,i){return[e[0]+(i[0]-e[0])*r,e[1]+(i[1]-e[1])*r,e[2]+(i[2]-e[2])*r]}static setCoordinate(e,r,i,o){let s=n.intercept(e[o],r,i[o]);return n.lerpPoint(e,s,i)}static isBounded(e){return 0<=e&&e<=100}static nthVertex(e,r){let i=n.Y_FROM_LINRGB[0],o=n.Y_FROM_LINRGB[1],s=n.Y_FROM_LINRGB[2],a=r%4<=1?0:100,c=r%2===0?0:100;if(r<4){let l=a,d=c,u=(e-l*o-d*s)/i;return n.isBounded(u)?[u,l,d]:[-1,-1,-1]}else if(r<8){let l=a,d=c,u=(e-d*i-l*s)/o;return n.isBounded(u)?[d,u,l]:[-1,-1,-1]}else{let l=a,d=c,u=(e-l*i-d*o)/s;return n.isBounded(u)?[l,d,u]:[-1,-1,-1]}}static bisectToSegment(e,r){let i=[-1,-1,-1],o=i,s=0,a=0,c=!1,l=!0;for(let d=0;d<12;d++){let u=n.nthVertex(e,d);if(u[0]<0)continue;let m=n.hueOf(u);if(!c){i=u,o=u,s=m,a=m,c=!0;continue}(l||n.areInCyclicOrder(s,m,a))&&(l=!1,n.areInCyclicOrder(s,r,m)?(o=u,a=m):(i=u,s=m))}return[i,o]}static midpoint(e,r){return[(e[0]+r[0])/2,(e[1]+r[1])/2,(e[2]+r[2])/2]}static criticalPlaneBelow(e){return Math.floor(e-.5)}static criticalPlaneAbove(e){return Math.ceil(e-.5)}static bisectToLimit(e,r){let i=n.bisectToSegment(e,r),o=i[0],s=n.hueOf(o),a=i[1];for(let c=0;c<3;c++)if(o[c]!==a[c]){let l=-1,d=255;o[c]<a[c]?(l=n.criticalPlaneBelow(n.trueDelinearized(o[c])),d=n.criticalPlaneAbove(n.trueDelinearized(a[c]))):(l=n.criticalPlaneAbove(n.trueDelinearized(o[c])),d=n.criticalPlaneBelow(n.trueDelinearized(a[c])));for(let u=0;u<8&&!(Math.abs(d-l)<=1);u++){let m=Math.floor((l+d)/2),h=n.CRITICAL_PLANES[m],p=n.setCoordinate(o,h,a,c),g=n.hueOf(p);n.areInCyclicOrder(s,r,g)?(a=p,d=m):(o=p,s=g,l=m)}}return n.midpoint(o,a)}static inverseChromaticAdaptation(e){let r=Math.abs(e),i=Math.max(0,27.13*r/(400-r));return Ct(e)*Math.pow(i,1/.42)}static findResultByJ(e,r,i){let o=Math.sqrt(i)*11,s=Ht.DEFAULT,a=1/Math.pow(1.64-Math.pow(.29,s.n),.73),l=.25*(Math.cos(e+2)+3.8)*(5e4/13)*s.nc*s.ncb,d=Math.sin(e),u=Math.cos(e);for(let m=0;m<5;m++){let h=o/100,p=r===0||o===0?0:r/Math.sqrt(h),g=Math.pow(p*a,1/.9),D=s.aw*Math.pow(h,1/s.c/s.z)/s.nbb,R=23*(D+.305)*g/(23*l+11*g*u+108*g*d),oe=R*u,Z=R*d,we=(460*D+451*oe+288*Z)/1403,me=(460*D-891*oe-261*Z)/1403,de=(460*D-220*oe-6300*Z)/1403,Je=n.inverseChromaticAdaptation(we),ut=n.inverseChromaticAdaptation(me),Tt=n.inverseChromaticAdaptation(de),ot=va([Je,ut,Tt],n.LINRGB_FROM_SCALED_DISCOUNT);if(ot[0]<0||ot[1]<0||ot[2]<0)return 0;let mr=n.Y_FROM_LINRGB[0],Zi=n.Y_FROM_LINRGB[1],hr=n.Y_FROM_LINRGB[2],qn=mr*ot[0]+Zi*ot[1]+hr*ot[2];if(qn<=0)return 0;if(m===4||Math.abs(qn-i)<.002)return ot[0]>100.01||ot[1]>100.01||ot[2]>100.01?0:Kp(ot);o=o-(qn-i)*o/(2*qn)}return 0}static solveToInt(e,r,i){if(r<1e-4||i<1e-4||i>99.9999)return LD(i);e=dt(e);let o=e/180*Math.PI,s=In(i),a=n.findResultByJ(o,r,s);if(a!==0)return a;let c=n.bisectToLimit(s,o);return Kp(c)}static solveToCam(e,r,i){return Wn.fromInt(n.solveToInt(e,r,i))}}return n.SCALED_DISCOUNT_FROM_LINRGB=[[.001200833568784504,.002389694492170889,.0002795742885861124],[.0005891086651375999,.0029785502573438758,.0003270666104008398],[.00010146692491640572,.0005364214359186694,.0032979401770712076]],n.LINRGB_FROM_SCALED_DISCOUNT=[[1373.2198709594231,-1100.4251190754821,-7.278681089101213],[-271.815969077903,559.6580465940733,-32.46047482791194],[1.9622899599665666,-57.173814538844006,308.7233197812385]],n.Y_FROM_LINRGB=[.2126,.7152,.0722],n.CRITICAL_PLANES=[.015176349177441876,.045529047532325624,.07588174588720938,.10623444424209313,.13658714259697685,.16693984095186062,.19729253930674434,.2276452376616281,.2579979360165119,.28835063437139563,.3188300904430532,.350925934958123,.3848314933096426,.42057480301049466,.458183274052838,.4976837250274023,.5391024159806381,.5824650784040898,.6277969426914107,.6751227633498623,.7244668422128921,.775853049866786,.829304845476233,.8848452951698498,.942497089126609,1.0022825574869039,1.0642236851973577,1.1283421258858297,1.1946592148522128,1.2631959812511864,1.3339731595349034,1.407011200216447,1.4823302800086415,1.5599503113873272,1.6398909516233677,1.7221716113234105,1.8068114625156377,1.8938294463134073,1.9832442801866852,2.075074464868551,2.1693382909216234,2.2660538449872063,2.36523901573795,2.4669114995532007,2.5710888059345764,2.6777882626779785,2.7870270208169257,2.898822059350997,3.0131901897720907,3.1301480604002863,3.2497121605402226,3.3718988244681087,3.4967242352587946,3.624204428461639,3.754355295633311,3.887192587735158,4.022731918402185,4.160988767090289,4.301978482107941,4.445716283538092,4.592217266055746,4.741496401646282,4.893568542229298,5.048448422192488,5.20615066083972,5.3666897647573375,5.5300801301023865,5.696336044816294,5.865471690767354,6.037501145825082,6.212438385869475,6.390297286737924,6.571091626112461,6.7548350853498045,6.941541251256611,7.131223617812143,7.323895587840543,7.5195704746346665,7.7182615035334345,7.919981813454504,8.124744458384042,8.332562408825165,8.543448553206703,8.757415699253682,8.974476575321063,9.194643831691977,9.417930041841839,9.644347703669503,9.873909240696694,10.106627003236781,10.342513269534024,10.58158024687427,10.8238400726681,11.069304815507364,11.317986476196008,11.569896988756009,11.825048221409341,12.083451977536606,12.345119996613247,12.610063955123938,12.878295467455942,13.149826086772048,13.42466730586372,13.702830557985108,13.984327217668513,14.269168601521828,14.55736596900856,14.848930523210871,15.143873411576273,15.44220572664832,15.743938506781891,16.04908273684337,16.35764934889634,16.66964922287304,16.985093187232053,17.30399201960269,17.62635644741625,17.95219714852476,18.281524751807332,18.614349837764564,18.95068293910138,19.290534541298456,19.633915083172692,19.98083495742689,20.331304511189067,20.685334046541502,21.042933821039977,21.404114048223256,21.76888489811322,22.137256497705877,22.50923893145328,22.884842241736916,23.264076429332462,23.6469514538663,24.033477234264016,24.42366364919083,24.817520537484558,25.21505769858089,25.61628489293138,26.021211842414342,26.429848230738664,26.842203703840827,27.258287870275353,27.678110301598522,28.10168053274597,28.529008062403893,28.96010235337422,29.39497283293396,29.83362889318845,30.276079891419332,30.722335150426627,31.172403958865512,31.62629557157785,32.08401920991837,32.54558406207592,33.010999283389665,33.4802739966603,33.953417292456834,34.430438229418264,34.911345834551085,35.39614910352207,35.88485700094671,36.37747846067349,36.87402238606382,37.37449765026789,37.87891309649659,38.38727753828926,38.89959975977785,39.41588851594697,39.93615253289054,40.460400508064545,40.98864111053629,41.520882981230194,42.05713473317016,42.597404951718396,43.141702194811224,43.6900349931913,44.24241185063697,44.798841244188324,45.35933162437017,45.92389141541209,46.49252901546552,47.065252796817916,47.64207110610409,48.22299226451468,48.808024568002054,49.3971762874833,49.9904556690408,50.587870934119984,51.189430279724725,51.79514187861014,52.40501387947288,53.0190544071392,53.637271562750364,54.259673423945976,54.88626804504493,55.517063457223934,56.15206766869424,56.79128866487574,57.43473440856916,58.08241284012621,58.734331877617365,59.39049941699807,60.05092333227251,60.715611475655585,61.38457167773311,62.057811747619894,62.7353394731159,63.417162620860914,64.10328893648692,64.79372614476921,65.48848194977529,66.18756403501224,66.89098006357258,67.59873767827808,68.31084450182222,69.02730813691093,69.74813616640164,70.47333615344107,71.20291564160104,71.93688215501312,72.67524319850172,73.41800625771542,74.16517879925733,74.9167682708136,75.67278210128072,76.43322770089146,77.1981124613393,77.96744375590167,78.74122893956174,79.51947534912904,80.30219030335869,81.08938110306934,81.88105503125999,82.67721935322541,83.4778813166706,84.28304815182372,85.09272707154808,85.90692527145302,86.72564993000343,87.54890820862819,88.3767072518277,89.2090541872801,90.04595612594655,90.88742016217518,91.73345337380438,92.58406282226491,93.43925555268066,94.29903859396902,95.16341895893969,96.03240364439274,96.9059996312159,97.78421388448044,98.6670533535366,99.55452497210776],n})();var O=class n{static from(t,e,r){return new n(ka.solveToInt(t,e,r))}static fromInt(t){return new n(t)}toInt(){return this.argb}get hue(){return this.internalHue}set hue(t){this.setInternalState(ka.solveToInt(t,this.internalChroma,this.internalTone))}get chroma(){return this.internalChroma}set chroma(t){this.setInternalState(ka.solveToInt(this.internalHue,t,this.internalTone))}get tone(){return this.internalTone}set tone(t){this.setInternalState(ka.solveToInt(this.internalHue,this.internalChroma,t))}setValue(t,e){this[t]=e}toString(){return`HCT(${this.hue.toFixed(0)}, ${this.chroma.toFixed(0)}, ${this.tone.toFixed(0)})`}static isBlue(t){return t>=250&&t<270}static isYellow(t){return t>=105&&t<125}static isCyan(t){return t>=170&&t<207}constructor(t){this.argb=t;let e=Wn.fromInt(t);this.internalHue=e.hue,this.internalChroma=e.chroma,this.internalTone=Hd(t),this.argb=t}setInternalState(t){let e=Wn.fromInt(t);this.internalHue=e.hue,this.internalChroma=e.chroma,this.internalTone=Hd(t),this.argb=t}inViewingConditions(t){let r=Wn.fromInt(this.toInt()).xyzInViewingConditions(t),i=Wn.fromXyzInViewingConditions(r[0],r[1],r[2],Ht.make());return n.from(i.hue,i.chroma,Ea(r[1]))}};var Xe=class n{static ratioOfTones(t,e){return t=nt(0,100,t),e=nt(0,100,e),n.ratioOfYs(In(t),In(e))}static ratioOfYs(t,e){let r=t>e?t:e,i=r===e?t:e;return(r+5)/(i+5)}static lighter(t,e){if(t<0||t>100)return-1;let r=In(t),i=e*(r+5)-5,o=n.ratioOfYs(i,r),s=Math.abs(o-e);if(o<e&&s>.04)return-1;let a=Ea(i)+.4;return a<0||a>100?-1:a}static darker(t,e){if(t<0||t>100)return-1;let r=In(t),i=(r+5)/e-5,o=n.ratioOfYs(r,i),s=Math.abs(o-e);if(o<e&&s>.04)return-1;let a=Ea(i)-.4;return a<0||a>100?-1:a}static lighterUnsafe(t,e){let r=n.lighter(t,e);return r<0?100:r}static darkerUnsafe(t,e){let r=n.darker(t,e);return r<0?0:r}};var Yi=class n{static isDisliked(t){let e=Math.round(t.hue)>=90&&Math.round(t.hue)<=111,r=Math.round(t.chroma)>16,i=Math.round(t.tone)<65;return e&&r&&i}static fixIfDisliked(t){return n.isDisliked(t)?O.from(t.hue,t.chroma,70):t}};function iO(n,t,e){if(n.name!==e.name)throw new Error(`Attempting to extend color ${n.name} with color ${e.name} of different name for spec version ${t}.`);if(n.isBackground!==e.isBackground)throw new Error(`Attempting to extend color ${n.name} as a ${n.isBackground?"background":"foreground"} with color ${e.name} as a ${e.isBackground?"background":"foreground"} for spec version ${t}.`)}function U(n,t,e){return iO(n,t,e),_.fromPalette({name:n.name,palette:r=>r.specVersion===t?e.palette(r):n.palette(r),tone:r=>r.specVersion===t?e.tone(r):n.tone(r),isBackground:n.isBackground,chromaMultiplier:r=>{let i=r.specVersion===t?e.chromaMultiplier:n.chromaMultiplier;return i!==void 0?i(r):1},background:r=>{let i=r.specVersion===t?e.background:n.background;return i!==void 0?i(r):void 0},secondBackground:r=>{let i=r.specVersion===t?e.secondBackground:n.secondBackground;return i!==void 0?i(r):void 0},contrastCurve:r=>{let i=r.specVersion===t?e.contrastCurve:n.contrastCurve;return i!==void 0?i(r):void 0},toneDeltaPair:r=>{let i=r.specVersion===t?e.toneDeltaPair:n.toneDeltaPair;return i!==void 0?i(r):void 0}})}var _=class n{static fromPalette(t){return new n(t.name??"",t.palette,t.tone??n.getInitialToneFromBackground(t.background),t.isBackground??!1,t.chromaMultiplier,t.background,t.secondBackground,t.contrastCurve,t.toneDeltaPair)}static getInitialToneFromBackground(t){return t===void 0?e=>50:e=>t(e)?t(e).getTone(e):50}constructor(t,e,r,i,o,s,a,c,l){if(this.name=t,this.palette=e,this.tone=r,this.isBackground=i,this.chromaMultiplier=o,this.background=s,this.secondBackground=a,this.contrastCurve=c,this.toneDeltaPair=l,this.hctCache=new Map,!s&&a)throw new Error(`Color ${t} has secondBackgrounddefined, but background is not defined.`);if(!s&&c)throw new Error(`Color ${t} has contrastCurvedefined, but background is not defined.`);if(s&&!c)throw new Error(`Color ${t} has backgrounddefined, but contrastCurve is not defined.`)}clone(){return n.fromPalette({name:this.name,palette:this.palette,tone:this.tone,isBackground:this.isBackground,chromaMultiplier:this.chromaMultiplier,background:this.background,secondBackground:this.secondBackground,contrastCurve:this.contrastCurve,toneDeltaPair:this.toneDeltaPair})}clearCache(){this.hctCache.clear()}getArgb(t){return this.getHct(t).toInt()}getHct(t){let e=this.hctCache.get(t);if(e!=null)return e;let r=VD(t.specVersion).getHct(t,this);return this.hctCache.size>4&&this.hctCache.clear(),this.hctCache.set(t,r),r}getTone(t){return VD(t.specVersion).getTone(t,this)}static foregroundTone(t,e){let r=Xe.lighterUnsafe(t,e),i=Xe.darkerUnsafe(t,e),o=Xe.ratioOfTones(r,t),s=Xe.ratioOfTones(i,t);if(n.tonePrefersLightForeground(t)){let c=Math.abs(o-s)<.1&&o<e&&s<e;return o>=e||o>=s||c?r:i}else return s>=e||s>=o?i:r}static tonePrefersLightForeground(t){return Math.round(t)<60}static toneAllowsLightForeground(t){return Math.round(t)<=49}static enableLightForeground(t){return n.tonePrefersLightForeground(t)&&!n.toneAllowsLightForeground(t)?49:t}},Xp=class{getHct(t,e){let r=e.getTone(t);return e.palette(t).getHct(r)}getTone(t,e){let r=t.contrastLevel<0,i=e.toneDeltaPair?e.toneDeltaPair(t):void 0;if(i){let o=i.roleA,s=i.roleB,a=i.delta,c=i.polarity,l=i.stayTogether,d=c==="nearer"||c==="lighter"&&!t.isDark||c==="darker"&&t.isDark,u=d?o:s,m=d?s:o,h=e.name===u.name,p=t.isDark?1:-1,g=u.tone(t),y=m.tone(t);if(e.background&&u.contrastCurve&&m.contrastCurve){let D=e.background(t),R=u.contrastCurve(t),oe=m.contrastCurve(t);if(D&&R&&oe){let Z=D.getTone(t),we=R.get(t.contrastLevel),me=oe.get(t.contrastLevel);Xe.ratioOfTones(Z,g)<we&&(g=_.foregroundTone(Z,we)),Xe.ratioOfTones(Z,y)<me&&(y=_.foregroundTone(Z,me)),r&&(g=_.foregroundTone(Z,we),y=_.foregroundTone(Z,me))}}return(y-g)*p<a&&(y=nt(0,100,g+a*p),(y-g)*p>=a||(g=nt(0,100,y-a*p))),50<=g&&g<60?p>0?(g=60,y=Math.max(y,g+a*p)):(g=49,y=Math.min(y,g+a*p)):50<=y&&y<60&&(l?p>0?(g=60,y=Math.max(y,g+a*p)):(g=49,y=Math.min(y,g+a*p)):p>0?y=60:y=49),h?g:y}else{let o=e.tone(t);if(e.background==null||e.background(t)===void 0||e.contrastCurve==null||e.contrastCurve(t)===void 0)return o;let s=e.background(t).getTone(t),a=e.contrastCurve(t).get(t.contrastLevel);if(Xe.ratioOfTones(s,o)>=a||(o=_.foregroundTone(s,a)),r&&(o=_.foregroundTone(s,a)),e.isBackground&&50<=o&&o<60&&(Xe.ratioOfTones(49,s)>=a?o=49:o=60),e.secondBackground==null||e.secondBackground(t)===void 0)return o;let[c,l]=[e.background,e.secondBackground],[d,u]=[c(t).getTone(t),l(t).getTone(t)],[m,h]=[Math.max(d,u),Math.min(d,u)];if(Xe.ratioOfTones(m,o)>=a&&Xe.ratioOfTones(h,o)>=a)return o;let p=Xe.lighter(m,a),g=Xe.darker(h,a),y=[];return p!==-1&&y.push(p),g!==-1&&y.push(g),_.tonePrefersLightForeground(d)||_.tonePrefersLightForeground(u)?p<0?100:p:y.length===1?y[0]:g<0?0:g}}},Jp=class{getHct(t,e){let r=e.palette(t),i=e.getTone(t),o=r.hue,s=r.chroma*(e.chromaMultiplier?e.chromaMultiplier(t):1);return O.from(o,s,i)}getTone(t,e){let r=e.toneDeltaPair?e.toneDeltaPair(t):void 0;if(r){let i=r.roleA,o=r.roleB,s=r.polarity,a=r.constraint,c=s==="darker"||s==="relative_lighter"&&t.isDark||s==="relative_darker"&&!t.isDark?-r.delta:r.delta,l=e.name===i.name,d=l?i:o,u=l?o:i,m=d.tone(t),h=u.getTone(t),p=c*(l?1:-1);if(a==="exact"?m=nt(0,100,h+p):a==="nearer"?p>0?m=nt(0,100,nt(h,h+p,m)):m=nt(0,100,nt(h+p,h,m)):a==="farther"&&(p>0?m=nt(h+p,100,m):m=nt(0,h+p,m)),e.background&&e.contrastCurve){let g=e.background(t),y=e.contrastCurve(t);if(g&&y){let D=g.getTone(t),R=y.get(t.contrastLevel);m=Xe.ratioOfTones(D,m)>=R&&t.contrastLevel>=0?m:_.foregroundTone(D,R)}}return e.isBackground&&!e.name.endsWith("_fixed_dim")&&(m>=57?m=nt(65,100,m):m=nt(0,49,m)),m}else{let i=e.tone(t);if(e.background==null||e.background(t)===void 0||e.contrastCurve==null||e.contrastCurve(t)===void 0)return i;let o=e.background(t).getTone(t),s=e.contrastCurve(t).get(t.contrastLevel);if(i=Xe.ratioOfTones(o,i)>=s&&t.contrastLevel>=0?i:_.foregroundTone(o,s),e.isBackground&&!e.name.endsWith("_fixed_dim")&&(i>=57?i=nt(65,100,i):i=nt(0,49,i)),e.secondBackground==null||e.secondBackground(t)===void 0)return i;let[a,c]=[e.background,e.secondBackground],[l,d]=[a(t).getTone(t),c(t).getTone(t)],[u,m]=[Math.max(l,d),Math.min(l,d)];if(Xe.ratioOfTones(u,i)>=s&&Xe.ratioOfTones(m,i)>=s)return i;let h=Xe.lighter(u,s),p=Xe.darker(m,s),g=[];return h!==-1&&g.push(h),p!==-1&&g.push(p),_.tonePrefersLightForeground(l)||_.tonePrefersLightForeground(d)?h<0?100:h:g.length===1?g[0]:p<0?0:p}}},oO=new Xp,sO=new Jp;function VD(n){return n==="2025"?sO:oO}var k=class n{static fromInt(t){let e=O.fromInt(t);return n.fromHct(e)}static fromHct(t){return new n(t.hue,t.chroma,t)}static fromHueAndChroma(t,e){let r=new eg(t,e).create();return new n(t,e,r)}constructor(t,e,r){this.hue=t,this.chroma=e,this.keyColor=r,this.cache=new Map}tone(t){let e=this.cache.get(t);return e===void 0&&(t==99&&O.isYellow(this.hue)?e=this.averageArgb(this.tone(98),this.tone(100)):e=O.from(this.hue,this.chroma,t).toInt(),this.cache.set(t,e)),e}getHct(t){return O.fromInt(this.tone(t))}averageArgb(t,e){let r=t>>>16&255,i=t>>>8&255,o=t&255,s=e>>>16&255,a=e>>>8&255,c=e&255,l=Math.round((r+s)/2),d=Math.round((i+a)/2),u=Math.round((o+c)/2);return(255<<24|(l&255)<<16|(d&255)<<8|u&255)>>>0}},eg=class{constructor(t,e){this.hue=t,this.requestedChroma=e,this.chromaCache=new Map,this.maxChromaValue=200}create(){let i=0,o=100;for(;i<o;){let s=Math.floor((i+o)/2),a=this.maxChroma(s)<this.maxChroma(s+1);if(this.maxChroma(s)>=this.requestedChroma-.01)if(Math.abs(i-50)<Math.abs(o-50))o=s;else{if(i===s)return O.from(this.hue,this.requestedChroma,i);i=s}else a?i=s+1:o=s}return O.from(this.hue,this.requestedChroma,i)}maxChroma(t){if(this.chromaCache.has(t))return this.chromaCache.get(t);let e=O.from(this.hue,this.maxChromaValue,t).chroma;return this.chromaCache.set(t,e),e}};var Ia=class n{constructor(t){this.input=t,this.hctsByTempCache=[],this.hctsByHueCache=[],this.tempsByHctCache=new Map,this.inputRelativeTemperatureCache=-1,this.complementCache=null}get hctsByTemp(){if(this.hctsByTempCache.length>0)return this.hctsByTempCache;let t=this.hctsByHue.concat([this.input]),e=this.tempsByHct;return t.sort((r,i)=>e.get(r)-e.get(i)),this.hctsByTempCache=t,t}get warmest(){return this.hctsByTemp[this.hctsByTemp.length-1]}get coldest(){return this.hctsByTemp[0]}analogous(t=5,e=12){let r=Math.round(this.input.hue),i=this.hctsByHue[r],o=this.relativeTemperature(i),s=[i],a=0;for(let p=0;p<360;p++){let g=Vd(r+p),y=this.hctsByHue[g],D=this.relativeTemperature(y),R=Math.abs(D-o);o=D,a+=R}let c=1,l=a/e,d=0;for(o=this.relativeTemperature(i);s.length<e;){let p=Vd(r+c),g=this.hctsByHue[p],y=this.relativeTemperature(g),D=Math.abs(y-o);d+=D;let R=s.length*l,oe=d>=R,Z=1;for(;oe&&s.length<e;){s.push(g);let we=(s.length+Z)*l;oe=d>=we,Z++}if(o=y,c++,c>360){for(;s.length<e;)s.push(g);break}}let u=[this.input],m=Math.floor((t-1)/2);for(let p=1;p<m+1;p++){let g=0-p;for(;g<0;)g=s.length+g;g>=s.length&&(g=g%s.length),u.splice(0,0,s[g])}let h=t-m-1;for(let p=1;p<h+1;p++){let g=p;for(;g<0;)g=s.length+g;g>=s.length&&(g=g%s.length),u.push(s[g])}return u}get complement(){if(this.complementCache!=null)return this.complementCache;let t=this.coldest.hue,e=this.tempsByHct.get(this.coldest),r=this.warmest.hue,o=this.tempsByHct.get(this.warmest)-e,s=n.isBetween(this.input.hue,t,r),a=s?r:t,c=s?t:r,l=1,d=1e3,u=this.hctsByHue[Math.round(this.input.hue)],m=1-this.inputRelativeTemperature;for(let h=0;h<=360;h+=1){let p=dt(a+l*h);if(!n.isBetween(p,a,c))continue;let g=this.hctsByHue[Math.round(p)],y=(this.tempsByHct.get(g)-e)/o,D=Math.abs(m-y);D<d&&(d=D,u=g)}return this.complementCache=u,this.complementCache}relativeTemperature(t){let e=this.tempsByHct.get(this.warmest)-this.tempsByHct.get(this.coldest),r=this.tempsByHct.get(t)-this.tempsByHct.get(this.coldest);return e===0?.5:r/e}get inputRelativeTemperature(){return this.inputRelativeTemperatureCache>=0?this.inputRelativeTemperatureCache:(this.inputRelativeTemperatureCache=this.relativeTemperature(this.input),this.inputRelativeTemperatureCache)}get tempsByHct(){if(this.tempsByHctCache.size>0)return this.tempsByHctCache;let t=this.hctsByHue.concat([this.input]),e=new Map;for(let r of t)e.set(r,n.rawTemperature(r));return this.tempsByHctCache=e,e}get hctsByHue(){if(this.hctsByHueCache.length>0)return this.hctsByHueCache;let t=[];for(let e=0;e<=360;e+=1){let r=O.from(e,this.input.chroma,this.input.tone);t.push(r)}return this.hctsByHueCache=t,this.hctsByHueCache}static isBetween(t,e,r){return e<r?e<=t&&t<=r:e<=t||t<=r}static rawTemperature(t){let e=Qp(t.toInt()),r=dt(Math.atan2(e[2],e[1])*180/Math.PI),i=Math.sqrt(e[1]*e[1]+e[2]*e[2]);return-.5+.02*Math.pow(i,1.07)*Math.cos(dt(r-50)*Math.PI/180)}};var A=class{constructor(t,e,r,i){this.low=t,this.normal=e,this.medium=r,this.high=i}get(t){return t<=-1?this.low:t<0?Wi(this.low,this.normal,(t- -1)/1):t<.5?Wi(this.normal,this.medium,(t-0)/.5):t<1?Wi(this.medium,this.high,(t-.5)/.5):this.high}};var xe=class{constructor(t,e,r,i,o,s){this.roleA=t,this.roleB=e,this.delta=r,this.polarity=i,this.stayTogether=o,this.constraint=s,this.constraint=s??"exact"}};var b=(function(n){return n[n.MONOCHROME=0]="MONOCHROME",n[n.NEUTRAL=1]="NEUTRAL",n[n.TONAL_SPOT=2]="TONAL_SPOT",n[n.VIBRANT=3]="VIBRANT",n[n.EXPRESSIVE=4]="EXPRESSIVE",n[n.FIDELITY=5]="FIDELITY",n[n.CONTENT=6]="CONTENT",n[n.RAINBOW=7]="RAINBOW",n[n.FRUIT_SALAD=8]="FRUIT_SALAD",n})(b||{});function Wo(n){return n.variant===b.FIDELITY||n.variant===b.CONTENT}function He(n){return n.variant===b.MONOCHROME}function aO(n,t,e,r){let i=e,o=O.from(n,t,e);if(o.chroma<t){let s=o.chroma;for(;o.chroma<t;){i+=r?-1:1;let a=O.from(n,t,i);if(s>a.chroma||Math.abs(a.chroma-t)<.4)break;let c=Math.abs(a.chroma-t),l=Math.abs(o.chroma-t);c<l&&(o=a),s=Math.max(s,a.chroma)}}return i}var zd=class{primaryPaletteKeyColor(){return _.fromPalette({name:"primary_palette_key_color",palette:t=>t.primaryPalette,tone:t=>t.primaryPalette.keyColor.tone})}secondaryPaletteKeyColor(){return _.fromPalette({name:"secondary_palette_key_color",palette:t=>t.secondaryPalette,tone:t=>t.secondaryPalette.keyColor.tone})}tertiaryPaletteKeyColor(){return _.fromPalette({name:"tertiary_palette_key_color",palette:t=>t.tertiaryPalette,tone:t=>t.tertiaryPalette.keyColor.tone})}neutralPaletteKeyColor(){return _.fromPalette({name:"neutral_palette_key_color",palette:t=>t.neutralPalette,tone:t=>t.neutralPalette.keyColor.tone})}neutralVariantPaletteKeyColor(){return _.fromPalette({name:"neutral_variant_palette_key_color",palette:t=>t.neutralVariantPalette,tone:t=>t.neutralVariantPalette.keyColor.tone})}errorPaletteKeyColor(){return _.fromPalette({name:"error_palette_key_color",palette:t=>t.errorPalette,tone:t=>t.errorPalette.keyColor.tone})}background(){return _.fromPalette({name:"background",palette:t=>t.neutralPalette,tone:t=>t.isDark?6:98,isBackground:!0})}onBackground(){return _.fromPalette({name:"on_background",palette:t=>t.neutralPalette,tone:t=>t.isDark?90:10,background:t=>this.background(),contrastCurve:t=>new A(3,3,4.5,7)})}surface(){return _.fromPalette({name:"surface",palette:t=>t.neutralPalette,tone:t=>t.isDark?6:98,isBackground:!0})}surfaceDim(){return _.fromPalette({name:"surface_dim",palette:t=>t.neutralPalette,tone:t=>t.isDark?6:new A(87,87,80,75).get(t.contrastLevel),isBackground:!0})}surfaceBright(){return _.fromPalette({name:"surface_bright",palette:t=>t.neutralPalette,tone:t=>t.isDark?new A(24,24,29,34).get(t.contrastLevel):98,isBackground:!0})}surfaceContainerLowest(){return _.fromPalette({name:"surface_container_lowest",palette:t=>t.neutralPalette,tone:t=>t.isDark?new A(4,4,2,0).get(t.contrastLevel):100,isBackground:!0})}surfaceContainerLow(){return _.fromPalette({name:"surface_container_low",palette:t=>t.neutralPalette,tone:t=>t.isDark?new A(10,10,11,12).get(t.contrastLevel):new A(96,96,96,95).get(t.contrastLevel),isBackground:!0})}surfaceContainer(){return _.fromPalette({name:"surface_container",palette:t=>t.neutralPalette,tone:t=>t.isDark?new A(12,12,16,20).get(t.contrastLevel):new A(94,94,92,90).get(t.contrastLevel),isBackground:!0})}surfaceContainerHigh(){return _.fromPalette({name:"surface_container_high",palette:t=>t.neutralPalette,tone:t=>t.isDark?new A(17,17,21,25).get(t.contrastLevel):new A(92,92,88,85).get(t.contrastLevel),isBackground:!0})}surfaceContainerHighest(){return _.fromPalette({name:"surface_container_highest",palette:t=>t.neutralPalette,tone:t=>t.isDark?new A(22,22,26,30).get(t.contrastLevel):new A(90,90,84,80).get(t.contrastLevel),isBackground:!0})}onSurface(){return _.fromPalette({name:"on_surface",palette:t=>t.neutralPalette,tone:t=>t.isDark?90:10,background:t=>this.highestSurface(t),contrastCurve:t=>new A(4.5,7,11,21)})}surfaceVariant(){return _.fromPalette({name:"surface_variant",palette:t=>t.neutralVariantPalette,tone:t=>t.isDark?30:90,isBackground:!0})}onSurfaceVariant(){return _.fromPalette({name:"on_surface_variant",palette:t=>t.neutralVariantPalette,tone:t=>t.isDark?80:30,background:t=>this.highestSurface(t),contrastCurve:t=>new A(3,4.5,7,11)})}inverseSurface(){return _.fromPalette({name:"inverse_surface",palette:t=>t.neutralPalette,tone:t=>t.isDark?90:20,isBackground:!0})}inverseOnSurface(){return _.fromPalette({name:"inverse_on_surface",palette:t=>t.neutralPalette,tone:t=>t.isDark?20:95,background:t=>this.inverseSurface(),contrastCurve:t=>new A(4.5,7,11,21)})}outline(){return _.fromPalette({name:"outline",palette:t=>t.neutralVariantPalette,tone:t=>t.isDark?60:50,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1.5,3,4.5,7)})}outlineVariant(){return _.fromPalette({name:"outline_variant",palette:t=>t.neutralVariantPalette,tone:t=>t.isDark?30:80,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5)})}shadow(){return _.fromPalette({name:"shadow",palette:t=>t.neutralPalette,tone:t=>0})}scrim(){return _.fromPalette({name:"scrim",palette:t=>t.neutralPalette,tone:t=>0})}surfaceTint(){return _.fromPalette({name:"surface_tint",palette:t=>t.primaryPalette,tone:t=>t.isDark?80:40,isBackground:!0})}primary(){return _.fromPalette({name:"primary",palette:t=>t.primaryPalette,tone:t=>He(t)?t.isDark?100:0:t.isDark?80:40,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(3,4.5,7,7),toneDeltaPair:t=>new xe(this.primaryContainer(),this.primary(),10,"nearer",!1)})}primaryDim(){}onPrimary(){return _.fromPalette({name:"on_primary",palette:t=>t.primaryPalette,tone:t=>He(t)?t.isDark?10:90:t.isDark?20:100,background:t=>this.primary(),contrastCurve:t=>new A(4.5,7,11,21)})}primaryContainer(){return _.fromPalette({name:"primary_container",palette:t=>t.primaryPalette,tone:t=>Wo(t)?t.sourceColorHct.tone:He(t)?t.isDark?85:25:t.isDark?30:90,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5),toneDeltaPair:t=>new xe(this.primaryContainer(),this.primary(),10,"nearer",!1)})}onPrimaryContainer(){return _.fromPalette({name:"on_primary_container",palette:t=>t.primaryPalette,tone:t=>Wo(t)?_.foregroundTone(this.primaryContainer().tone(t),4.5):He(t)?t.isDark?0:100:t.isDark?90:30,background:t=>this.primaryContainer(),contrastCurve:t=>new A(3,4.5,7,11)})}inversePrimary(){return _.fromPalette({name:"inverse_primary",palette:t=>t.primaryPalette,tone:t=>t.isDark?40:80,background:t=>this.inverseSurface(),contrastCurve:t=>new A(3,4.5,7,7)})}secondary(){return _.fromPalette({name:"secondary",palette:t=>t.secondaryPalette,tone:t=>t.isDark?80:40,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(3,4.5,7,7),toneDeltaPair:t=>new xe(this.secondaryContainer(),this.secondary(),10,"nearer",!1)})}secondaryDim(){}onSecondary(){return _.fromPalette({name:"on_secondary",palette:t=>t.secondaryPalette,tone:t=>He(t)?t.isDark?10:100:t.isDark?20:100,background:t=>this.secondary(),contrastCurve:t=>new A(4.5,7,11,21)})}secondaryContainer(){return _.fromPalette({name:"secondary_container",palette:t=>t.secondaryPalette,tone:t=>{let e=t.isDark?30:90;return He(t)?t.isDark?30:85:Wo(t)?aO(t.secondaryPalette.hue,t.secondaryPalette.chroma,e,!t.isDark):e},isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5),toneDeltaPair:t=>new xe(this.secondaryContainer(),this.secondary(),10,"nearer",!1)})}onSecondaryContainer(){return _.fromPalette({name:"on_secondary_container",palette:t=>t.secondaryPalette,tone:t=>He(t)?t.isDark?90:10:Wo(t)?_.foregroundTone(this.secondaryContainer().tone(t),4.5):t.isDark?90:30,background:t=>this.secondaryContainer(),contrastCurve:t=>new A(3,4.5,7,11)})}tertiary(){return _.fromPalette({name:"tertiary",palette:t=>t.tertiaryPalette,tone:t=>He(t)?t.isDark?90:25:t.isDark?80:40,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(3,4.5,7,7),toneDeltaPair:t=>new xe(this.tertiaryContainer(),this.tertiary(),10,"nearer",!1)})}tertiaryDim(){}onTertiary(){return _.fromPalette({name:"on_tertiary",palette:t=>t.tertiaryPalette,tone:t=>He(t)?t.isDark?10:90:t.isDark?20:100,background:t=>this.tertiary(),contrastCurve:t=>new A(4.5,7,11,21)})}tertiaryContainer(){return _.fromPalette({name:"tertiary_container",palette:t=>t.tertiaryPalette,tone:t=>{if(He(t))return t.isDark?60:49;if(!Wo(t))return t.isDark?30:90;let e=t.tertiaryPalette.getHct(t.sourceColorHct.tone);return Yi.fixIfDisliked(e).tone},isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5),toneDeltaPair:t=>new xe(this.tertiaryContainer(),this.tertiary(),10,"nearer",!1)})}onTertiaryContainer(){return _.fromPalette({name:"on_tertiary_container",palette:t=>t.tertiaryPalette,tone:t=>He(t)?t.isDark?0:100:Wo(t)?_.foregroundTone(this.tertiaryContainer().tone(t),4.5):t.isDark?90:30,background:t=>this.tertiaryContainer(),contrastCurve:t=>new A(3,4.5,7,11)})}error(){return _.fromPalette({name:"error",palette:t=>t.errorPalette,tone:t=>t.isDark?80:40,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(3,4.5,7,7),toneDeltaPair:t=>new xe(this.errorContainer(),this.error(),10,"nearer",!1)})}errorDim(){}onError(){return _.fromPalette({name:"on_error",palette:t=>t.errorPalette,tone:t=>t.isDark?20:100,background:t=>this.error(),contrastCurve:t=>new A(4.5,7,11,21)})}errorContainer(){return _.fromPalette({name:"error_container",palette:t=>t.errorPalette,tone:t=>t.isDark?30:90,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5),toneDeltaPair:t=>new xe(this.errorContainer(),this.error(),10,"nearer",!1)})}onErrorContainer(){return _.fromPalette({name:"on_error_container",palette:t=>t.errorPalette,tone:t=>He(t)?t.isDark?90:10:t.isDark?90:30,background:t=>this.errorContainer(),contrastCurve:t=>new A(3,4.5,7,11)})}primaryFixed(){return _.fromPalette({name:"primary_fixed",palette:t=>t.primaryPalette,tone:t=>He(t)?40:90,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5),toneDeltaPair:t=>new xe(this.primaryFixed(),this.primaryFixedDim(),10,"lighter",!0)})}primaryFixedDim(){return _.fromPalette({name:"primary_fixed_dim",palette:t=>t.primaryPalette,tone:t=>He(t)?30:80,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5),toneDeltaPair:t=>new xe(this.primaryFixed(),this.primaryFixedDim(),10,"lighter",!0)})}onPrimaryFixed(){return _.fromPalette({name:"on_primary_fixed",palette:t=>t.primaryPalette,tone:t=>He(t)?100:10,background:t=>this.primaryFixedDim(),secondBackground:t=>this.primaryFixed(),contrastCurve:t=>new A(4.5,7,11,21)})}onPrimaryFixedVariant(){return _.fromPalette({name:"on_primary_fixed_variant",palette:t=>t.primaryPalette,tone:t=>He(t)?90:30,background:t=>this.primaryFixedDim(),secondBackground:t=>this.primaryFixed(),contrastCurve:t=>new A(3,4.5,7,11)})}secondaryFixed(){return _.fromPalette({name:"secondary_fixed",palette:t=>t.secondaryPalette,tone:t=>He(t)?80:90,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5),toneDeltaPair:t=>new xe(this.secondaryFixed(),this.secondaryFixedDim(),10,"lighter",!0)})}secondaryFixedDim(){return _.fromPalette({name:"secondary_fixed_dim",palette:t=>t.secondaryPalette,tone:t=>He(t)?70:80,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5),toneDeltaPair:t=>new xe(this.secondaryFixed(),this.secondaryFixedDim(),10,"lighter",!0)})}onSecondaryFixed(){return _.fromPalette({name:"on_secondary_fixed",palette:t=>t.secondaryPalette,tone:t=>10,background:t=>this.secondaryFixedDim(),secondBackground:t=>this.secondaryFixed(),contrastCurve:t=>new A(4.5,7,11,21)})}onSecondaryFixedVariant(){return _.fromPalette({name:"on_secondary_fixed_variant",palette:t=>t.secondaryPalette,tone:t=>He(t)?25:30,background:t=>this.secondaryFixedDim(),secondBackground:t=>this.secondaryFixed(),contrastCurve:t=>new A(3,4.5,7,11)})}tertiaryFixed(){return _.fromPalette({name:"tertiary_fixed",palette:t=>t.tertiaryPalette,tone:t=>He(t)?40:90,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5),toneDeltaPair:t=>new xe(this.tertiaryFixed(),this.tertiaryFixedDim(),10,"lighter",!0)})}tertiaryFixedDim(){return _.fromPalette({name:"tertiary_fixed_dim",palette:t=>t.tertiaryPalette,tone:t=>He(t)?30:80,isBackground:!0,background:t=>this.highestSurface(t),contrastCurve:t=>new A(1,1,3,4.5),toneDeltaPair:t=>new xe(this.tertiaryFixed(),this.tertiaryFixedDim(),10,"lighter",!0)})}onTertiaryFixed(){return _.fromPalette({name:"on_tertiary_fixed",palette:t=>t.tertiaryPalette,tone:t=>He(t)?100:10,background:t=>this.tertiaryFixedDim(),secondBackground:t=>this.tertiaryFixed(),contrastCurve:t=>new A(4.5,7,11,21)})}onTertiaryFixedVariant(){return _.fromPalette({name:"on_tertiary_fixed_variant",palette:t=>t.tertiaryPalette,tone:t=>He(t)?90:30,background:t=>this.tertiaryFixedDim(),secondBackground:t=>this.tertiaryFixed(),contrastCurve:t=>new A(3,4.5,7,11)})}highestSurface(t){return t.isDark?this.surfaceBright():this.surfaceDim()}};function ce(n,t=0,e=100,r=1){let i=jD(n.hue,n.chroma*r,100,!0);return nt(t,e,i)}function zr(n,t=0,e=100){let r=jD(n.hue,n.chroma,0,!1);return nt(t,e,r)}function jD(n,t,e,r){let i=e,o=O.from(n,t,i);for(;o.chroma<t&&!(e<0||e>100);){e+=r?-1:1;let s=O.from(n,t,e);o.chroma<s.chroma&&(o=s,i=e)}return i}function P(n){return n===1.5?new A(1.5,1.5,3,5.5):n===3?new A(3,3,4.5,7):n===4.5?new A(4.5,4.5,7,11):n===6?new A(6,6,7,11):n===7?new A(7,7,11,21):n===9?new A(9,9,11,21):n===11?new A(11,11,21,21):n===21?new A(21,21,21,21):new A(n,n,7,21)}var Ud=class extends zd{surface(){let t=_.fromPalette({name:"surface",palette:e=>e.neutralPalette,tone:e=>(super.surface().tone(e),e.platform==="phone"?e.isDark?4:O.isYellow(e.neutralPalette.hue)?99:e.variant===b.VIBRANT?97:98:0),isBackground:!0});return U(super.surface(),"2025",t)}surfaceDim(){let t=_.fromPalette({name:"surface_dim",palette:e=>e.neutralPalette,tone:e=>e.isDark?4:O.isYellow(e.neutralPalette.hue)?90:e.variant===b.VIBRANT?85:87,isBackground:!0,chromaMultiplier:e=>{if(!e.isDark){if(e.variant===b.NEUTRAL)return 2.5;if(e.variant===b.TONAL_SPOT)return 1.7;if(e.variant===b.EXPRESSIVE)return O.isYellow(e.neutralPalette.hue)?2.7:1.75;if(e.variant===b.VIBRANT)return 1.36}return 1}});return U(super.surfaceDim(),"2025",t)}surfaceBright(){let t=_.fromPalette({name:"surface_bright",palette:e=>e.neutralPalette,tone:e=>e.isDark?18:O.isYellow(e.neutralPalette.hue)?99:e.variant===b.VIBRANT?97:98,isBackground:!0,chromaMultiplier:e=>{if(e.isDark){if(e.variant===b.NEUTRAL)return 2.5;if(e.variant===b.TONAL_SPOT)return 1.7;if(e.variant===b.EXPRESSIVE)return O.isYellow(e.neutralPalette.hue)?2.7:1.75;if(e.variant===b.VIBRANT)return 1.36}return 1}});return U(super.surfaceBright(),"2025",t)}surfaceContainerLowest(){let t=_.fromPalette({name:"surface_container_lowest",palette:e=>e.neutralPalette,tone:e=>e.isDark?0:100,isBackground:!0});return U(super.surfaceContainerLowest(),"2025",t)}surfaceContainerLow(){let t=_.fromPalette({name:"surface_container_low",palette:e=>e.neutralPalette,tone:e=>e.platform==="phone"?e.isDark?6:O.isYellow(e.neutralPalette.hue)?98:e.variant===b.VIBRANT?95:96:15,isBackground:!0,chromaMultiplier:e=>{if(e.platform==="phone"){if(e.variant===b.NEUTRAL)return 1.3;if(e.variant===b.TONAL_SPOT)return 1.25;if(e.variant===b.EXPRESSIVE)return O.isYellow(e.neutralPalette.hue)?1.3:1.15;if(e.variant===b.VIBRANT)return 1.08}return 1}});return U(super.surfaceContainerLow(),"2025",t)}surfaceContainer(){let t=_.fromPalette({name:"surface_container",palette:e=>e.neutralPalette,tone:e=>e.platform==="phone"?e.isDark?9:O.isYellow(e.neutralPalette.hue)?96:e.variant===b.VIBRANT?92:94:20,isBackground:!0,chromaMultiplier:e=>{if(e.platform==="phone"){if(e.variant===b.NEUTRAL)return 1.6;if(e.variant===b.TONAL_SPOT)return 1.4;if(e.variant===b.EXPRESSIVE)return O.isYellow(e.neutralPalette.hue)?1.6:1.3;if(e.variant===b.VIBRANT)return 1.15}return 1}});return U(super.surfaceContainer(),"2025",t)}surfaceContainerHigh(){let t=_.fromPalette({name:"surface_container_high",palette:e=>e.neutralPalette,tone:e=>e.platform==="phone"?e.isDark?12:O.isYellow(e.neutralPalette.hue)?94:e.variant===b.VIBRANT?90:92:25,isBackground:!0,chromaMultiplier:e=>{if(e.platform==="phone"){if(e.variant===b.NEUTRAL)return 1.9;if(e.variant===b.TONAL_SPOT)return 1.5;if(e.variant===b.EXPRESSIVE)return O.isYellow(e.neutralPalette.hue)?1.95:1.45;if(e.variant===b.VIBRANT)return 1.22}return 1}});return U(super.surfaceContainerHigh(),"2025",t)}surfaceContainerHighest(){let t=_.fromPalette({name:"surface_container_highest",palette:e=>e.neutralPalette,tone:e=>e.isDark?15:O.isYellow(e.neutralPalette.hue)?92:e.variant===b.VIBRANT?88:90,isBackground:!0,chromaMultiplier:e=>e.variant===b.NEUTRAL?2.2:e.variant===b.TONAL_SPOT?1.7:e.variant===b.EXPRESSIVE?O.isYellow(e.neutralPalette.hue)?2.3:1.6:e.variant===b.VIBRANT?1.29:1});return U(super.surfaceContainerHighest(),"2025",t)}onSurface(){let t=_.fromPalette({name:"on_surface",palette:e=>e.neutralPalette,tone:e=>e.variant===b.VIBRANT?ce(e.neutralPalette,0,100,1.1):_.getInitialToneFromBackground(r=>r.platform==="phone"?this.highestSurface(r):this.surfaceContainerHigh())(e),chromaMultiplier:e=>{if(e.platform==="phone"){if(e.variant===b.NEUTRAL)return 2.2;if(e.variant===b.TONAL_SPOT)return 1.7;if(e.variant===b.EXPRESSIVE)return O.isYellow(e.neutralPalette.hue)?e.isDark?3:2.3:1.6}return 1},background:e=>e.platform==="phone"?this.highestSurface(e):this.surfaceContainerHigh(),contrastCurve:e=>e.isDark&&e.platform==="phone"?P(11):P(9)});return U(super.onSurface(),"2025",t)}onSurfaceVariant(){let t=_.fromPalette({name:"on_surface_variant",palette:e=>e.neutralPalette,chromaMultiplier:e=>{if(e.platform==="phone"){if(e.variant===b.NEUTRAL)return 2.2;if(e.variant===b.TONAL_SPOT)return 1.7;if(e.variant===b.EXPRESSIVE)return O.isYellow(e.neutralPalette.hue)?e.isDark?3:2.3:1.6}return 1},background:e=>e.platform==="phone"?this.highestSurface(e):this.surfaceContainerHigh(),contrastCurve:e=>e.platform==="phone"?e.isDark?P(6):P(4.5):P(7)});return U(super.onSurfaceVariant(),"2025",t)}outline(){let t=_.fromPalette({name:"outline",palette:e=>e.neutralPalette,chromaMultiplier:e=>{if(e.platform==="phone"){if(e.variant===b.NEUTRAL)return 2.2;if(e.variant===b.TONAL_SPOT)return 1.7;if(e.variant===b.EXPRESSIVE)return O.isYellow(e.neutralPalette.hue)?e.isDark?3:2.3:1.6}return 1},background:e=>e.platform==="phone"?this.highestSurface(e):this.surfaceContainerHigh(),contrastCurve:e=>e.platform==="phone"?P(3):P(4.5)});return U(super.outline(),"2025",t)}outlineVariant(){let t=_.fromPalette({name:"outline_variant",palette:e=>e.neutralPalette,chromaMultiplier:e=>{if(e.platform==="phone"){if(e.variant===b.NEUTRAL)return 2.2;if(e.variant===b.TONAL_SPOT)return 1.7;if(e.variant===b.EXPRESSIVE)return O.isYellow(e.neutralPalette.hue)?e.isDark?3:2.3:1.6}return 1},background:e=>e.platform==="phone"?this.highestSurface(e):this.surfaceContainerHigh(),contrastCurve:e=>e.platform==="phone"?P(1.5):P(3)});return U(super.outlineVariant(),"2025",t)}inverseSurface(){let t=_.fromPalette({name:"inverse_surface",palette:e=>e.neutralPalette,tone:e=>e.isDark?98:4,isBackground:!0});return U(super.inverseSurface(),"2025",t)}inverseOnSurface(){let t=_.fromPalette({name:"inverse_on_surface",palette:e=>e.neutralPalette,background:e=>this.inverseSurface(),contrastCurve:e=>P(7)});return U(super.inverseOnSurface(),"2025",t)}primary(){let t=_.fromPalette({name:"primary",palette:e=>e.primaryPalette,tone:e=>e.variant===b.NEUTRAL?e.platform==="phone"?e.isDark?80:40:90:e.variant===b.TONAL_SPOT?e.platform==="phone"?e.isDark?80:ce(e.primaryPalette):ce(e.primaryPalette,0,90):e.variant===b.EXPRESSIVE?e.platform==="phone"?ce(e.primaryPalette,0,O.isYellow(e.primaryPalette.hue)?25:O.isCyan(e.primaryPalette.hue)?88:98):ce(e.primaryPalette):e.platform==="phone"?ce(e.primaryPalette,0,O.isCyan(e.primaryPalette.hue)?88:98):ce(e.primaryPalette),isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):this.surfaceContainerHigh(),contrastCurve:e=>e.platform==="phone"?P(4.5):P(7),toneDeltaPair:e=>e.platform==="phone"?new xe(this.primaryContainer(),this.primary(),5,"relative_lighter",!0,"farther"):void 0});return U(super.primary(),"2025",t)}primaryDim(){return _.fromPalette({name:"primary_dim",palette:t=>t.primaryPalette,tone:t=>t.variant===b.NEUTRAL?85:t.variant===b.TONAL_SPOT?ce(t.primaryPalette,0,90):ce(t.primaryPalette),isBackground:!0,background:t=>this.surfaceContainerHigh(),contrastCurve:t=>P(4.5),toneDeltaPair:t=>new xe(this.primaryDim(),this.primary(),5,"darker",!0,"farther")})}onPrimary(){let t=_.fromPalette({name:"on_primary",palette:e=>e.primaryPalette,background:e=>e.platform==="phone"?this.primary():this.primaryDim(),contrastCurve:e=>e.platform==="phone"?P(6):P(7)});return U(super.onPrimary(),"2025",t)}primaryContainer(){let t=_.fromPalette({name:"primary_container",palette:e=>e.primaryPalette,tone:e=>e.platform==="watch"?30:e.variant===b.NEUTRAL?e.isDark?30:90:e.variant===b.TONAL_SPOT?e.isDark?zr(e.primaryPalette,35,93):ce(e.primaryPalette,0,90):e.variant===b.EXPRESSIVE?e.isDark?ce(e.primaryPalette,30,93):ce(e.primaryPalette,78,O.isCyan(e.primaryPalette.hue)?88:90):e.isDark?zr(e.primaryPalette,66,93):ce(e.primaryPalette,66,O.isCyan(e.primaryPalette.hue)?88:93),isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):void 0,toneDeltaPair:e=>e.platform==="phone"?void 0:new xe(this.primaryContainer(),this.primaryDim(),10,"darker",!0,"farther"),contrastCurve:e=>e.platform==="phone"&&e.contrastLevel>0?P(1.5):void 0});return U(super.primaryContainer(),"2025",t)}onPrimaryContainer(){let t=_.fromPalette({name:"on_primary_container",palette:e=>e.primaryPalette,background:e=>this.primaryContainer(),contrastCurve:e=>e.platform==="phone"?P(6):P(7)});return U(super.onPrimaryContainer(),"2025",t)}primaryFixed(){let t=_.fromPalette({name:"primary_fixed",palette:e=>e.primaryPalette,tone:e=>{let r=Object.assign({},e,{isDark:!1,contrastLevel:0});return this.primaryContainer().getTone(r)},isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):void 0,contrastCurve:e=>e.platform==="phone"&&e.contrastLevel>0?P(1.5):void 0});return U(super.primaryFixed(),"2025",t)}primaryFixedDim(){let t=_.fromPalette({name:"primary_fixed_dim",palette:e=>e.primaryPalette,tone:e=>this.primaryFixed().getTone(e),isBackground:!0,toneDeltaPair:e=>new xe(this.primaryFixedDim(),this.primaryFixed(),5,"darker",!0,"exact")});return U(super.primaryFixedDim(),"2025",t)}onPrimaryFixed(){let t=_.fromPalette({name:"on_primary_fixed",palette:e=>e.primaryPalette,background:e=>this.primaryFixedDim(),contrastCurve:e=>P(7)});return U(super.onPrimaryFixed(),"2025",t)}onPrimaryFixedVariant(){let t=_.fromPalette({name:"on_primary_fixed_variant",palette:e=>e.primaryPalette,background:e=>this.primaryFixedDim(),contrastCurve:e=>P(4.5)});return U(super.onPrimaryFixedVariant(),"2025",t)}inversePrimary(){let t=_.fromPalette({name:"inverse_primary",palette:e=>e.primaryPalette,tone:e=>ce(e.primaryPalette),background:e=>this.inverseSurface(),contrastCurve:e=>e.platform==="phone"?P(6):P(7)});return U(super.inversePrimary(),"2025",t)}secondary(){let t=_.fromPalette({name:"secondary",palette:e=>e.secondaryPalette,tone:e=>e.platform==="watch"?e.variant===b.NEUTRAL?90:ce(e.secondaryPalette,0,90):e.variant===b.NEUTRAL?e.isDark?zr(e.secondaryPalette,0,98):ce(e.secondaryPalette):e.variant===b.VIBRANT?ce(e.secondaryPalette,0,e.isDark?90:98):e.isDark?80:ce(e.secondaryPalette),isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):this.surfaceContainerHigh(),contrastCurve:e=>e.platform==="phone"?P(4.5):P(7),toneDeltaPair:e=>e.platform==="phone"?new xe(this.secondaryContainer(),this.secondary(),5,"relative_lighter",!0,"farther"):void 0});return U(super.secondary(),"2025",t)}secondaryDim(){return _.fromPalette({name:"secondary_dim",palette:t=>t.secondaryPalette,tone:t=>t.variant===b.NEUTRAL?85:ce(t.secondaryPalette,0,90),isBackground:!0,background:t=>this.surfaceContainerHigh(),contrastCurve:t=>P(4.5),toneDeltaPair:t=>new xe(this.secondaryDim(),this.secondary(),5,"darker",!0,"farther")})}onSecondary(){let t=_.fromPalette({name:"on_secondary",palette:e=>e.secondaryPalette,background:e=>e.platform==="phone"?this.secondary():this.secondaryDim(),contrastCurve:e=>e.platform==="phone"?P(6):P(7)});return U(super.onSecondary(),"2025",t)}secondaryContainer(){let t=_.fromPalette({name:"secondary_container",palette:e=>e.secondaryPalette,tone:e=>e.platform==="watch"?30:e.variant===b.VIBRANT?e.isDark?zr(e.secondaryPalette,30,40):ce(e.secondaryPalette,84,90):e.variant===b.EXPRESSIVE?e.isDark?15:ce(e.secondaryPalette,90,95):e.isDark?25:90,isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):void 0,toneDeltaPair:e=>e.platform==="watch"?new xe(this.secondaryContainer(),this.secondaryDim(),10,"darker",!0,"farther"):void 0,contrastCurve:e=>e.platform==="phone"&&e.contrastLevel>0?P(1.5):void 0});return U(super.secondaryContainer(),"2025",t)}onSecondaryContainer(){let t=_.fromPalette({name:"on_secondary_container",palette:e=>e.secondaryPalette,background:e=>this.secondaryContainer(),contrastCurve:e=>e.platform==="phone"?P(6):P(7)});return U(super.onSecondaryContainer(),"2025",t)}secondaryFixed(){let t=_.fromPalette({name:"secondary_fixed",palette:e=>e.secondaryPalette,tone:e=>{let r=Object.assign({},e,{isDark:!1,contrastLevel:0});return this.secondaryContainer().getTone(r)},isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):void 0,contrastCurve:e=>e.platform==="phone"&&e.contrastLevel>0?P(1.5):void 0});return U(super.secondaryFixed(),"2025",t)}secondaryFixedDim(){let t=_.fromPalette({name:"secondary_fixed_dim",palette:e=>e.secondaryPalette,tone:e=>this.secondaryFixed().getTone(e),isBackground:!0,toneDeltaPair:e=>new xe(this.secondaryFixedDim(),this.secondaryFixed(),5,"darker",!0,"exact")});return U(super.secondaryFixedDim(),"2025",t)}onSecondaryFixed(){let t=_.fromPalette({name:"on_secondary_fixed",palette:e=>e.secondaryPalette,background:e=>this.secondaryFixedDim(),contrastCurve:e=>P(7)});return U(super.onSecondaryFixed(),"2025",t)}onSecondaryFixedVariant(){let t=_.fromPalette({name:"on_secondary_fixed_variant",palette:e=>e.secondaryPalette,background:e=>this.secondaryFixedDim(),contrastCurve:e=>P(4.5)});return U(super.onSecondaryFixedVariant(),"2025",t)}tertiary(){let t=_.fromPalette({name:"tertiary",palette:e=>e.tertiaryPalette,tone:e=>e.platform==="watch"?e.variant===b.TONAL_SPOT?ce(e.tertiaryPalette,0,90):ce(e.tertiaryPalette):e.variant===b.EXPRESSIVE||e.variant===b.VIBRANT?ce(e.tertiaryPalette,0,O.isCyan(e.tertiaryPalette.hue)?88:e.isDark?98:100):e.isDark?ce(e.tertiaryPalette,0,98):ce(e.tertiaryPalette),isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):this.surfaceContainerHigh(),contrastCurve:e=>e.platform==="phone"?P(4.5):P(7),toneDeltaPair:e=>e.platform==="phone"?new xe(this.tertiaryContainer(),this.tertiary(),5,"relative_lighter",!0,"farther"):void 0});return U(super.tertiary(),"2025",t)}tertiaryDim(){return _.fromPalette({name:"tertiary_dim",palette:t=>t.tertiaryPalette,tone:t=>t.variant===b.TONAL_SPOT?ce(t.tertiaryPalette,0,90):ce(t.tertiaryPalette),isBackground:!0,background:t=>this.surfaceContainerHigh(),contrastCurve:t=>P(4.5),toneDeltaPair:t=>new xe(this.tertiaryDim(),this.tertiary(),5,"darker",!0,"farther")})}onTertiary(){let t=_.fromPalette({name:"on_tertiary",palette:e=>e.tertiaryPalette,background:e=>e.platform==="phone"?this.tertiary():this.tertiaryDim(),contrastCurve:e=>e.platform==="phone"?P(6):P(7)});return U(super.onTertiary(),"2025",t)}tertiaryContainer(){let t=_.fromPalette({name:"tertiary_container",palette:e=>e.tertiaryPalette,tone:e=>e.platform==="watch"?e.variant===b.TONAL_SPOT?ce(e.tertiaryPalette,0,90):ce(e.tertiaryPalette):e.variant===b.NEUTRAL?e.isDark?ce(e.tertiaryPalette,0,93):ce(e.tertiaryPalette,0,96):e.variant===b.TONAL_SPOT?ce(e.tertiaryPalette,0,e.isDark?93:100):e.variant===b.EXPRESSIVE?ce(e.tertiaryPalette,75,O.isCyan(e.tertiaryPalette.hue)?88:e.isDark?93:100):e.isDark?ce(e.tertiaryPalette,0,93):ce(e.tertiaryPalette,72,100),isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):void 0,toneDeltaPair:e=>e.platform==="watch"?new xe(this.tertiaryContainer(),this.tertiaryDim(),10,"darker",!0,"farther"):void 0,contrastCurve:e=>e.platform==="phone"&&e.contrastLevel>0?P(1.5):void 0});return U(super.tertiaryContainer(),"2025",t)}onTertiaryContainer(){let t=_.fromPalette({name:"on_tertiary_container",palette:e=>e.tertiaryPalette,background:e=>this.tertiaryContainer(),contrastCurve:e=>e.platform==="phone"?P(6):P(7)});return U(super.onTertiaryContainer(),"2025",t)}tertiaryFixed(){let t=_.fromPalette({name:"tertiary_fixed",palette:e=>e.tertiaryPalette,tone:e=>{let r=Object.assign({},e,{isDark:!1,contrastLevel:0});return this.tertiaryContainer().getTone(r)},isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):void 0,contrastCurve:e=>e.platform==="phone"&&e.contrastLevel>0?P(1.5):void 0});return U(super.tertiaryFixed(),"2025",t)}tertiaryFixedDim(){let t=_.fromPalette({name:"tertiary_fixed_dim",palette:e=>e.tertiaryPalette,tone:e=>this.tertiaryFixed().getTone(e),isBackground:!0,toneDeltaPair:e=>new xe(this.tertiaryFixedDim(),this.tertiaryFixed(),5,"darker",!0,"exact")});return U(super.tertiaryFixedDim(),"2025",t)}onTertiaryFixed(){let t=_.fromPalette({name:"on_tertiary_fixed",palette:e=>e.tertiaryPalette,background:e=>this.tertiaryFixedDim(),contrastCurve:e=>P(7)});return U(super.onTertiaryFixed(),"2025",t)}onTertiaryFixedVariant(){let t=_.fromPalette({name:"on_tertiary_fixed_variant",palette:e=>e.tertiaryPalette,background:e=>this.tertiaryFixedDim(),contrastCurve:e=>P(4.5)});return U(super.onTertiaryFixedVariant(),"2025",t)}error(){let t=_.fromPalette({name:"error",palette:e=>e.errorPalette,tone:e=>e.platform==="phone"?e.isDark?zr(e.errorPalette,0,98):ce(e.errorPalette):zr(e.errorPalette),isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):this.surfaceContainerHigh(),contrastCurve:e=>e.platform==="phone"?P(4.5):P(7),toneDeltaPair:e=>e.platform==="phone"?new xe(this.errorContainer(),this.error(),5,"relative_lighter",!0,"farther"):void 0});return U(super.error(),"2025",t)}errorDim(){return _.fromPalette({name:"error_dim",palette:t=>t.errorPalette,tone:t=>zr(t.errorPalette),isBackground:!0,background:t=>this.surfaceContainerHigh(),contrastCurve:t=>P(4.5),toneDeltaPair:t=>new xe(this.errorDim(),this.error(),5,"darker",!0,"farther")})}onError(){let t=_.fromPalette({name:"on_error",palette:e=>e.errorPalette,background:e=>e.platform==="phone"?this.error():this.errorDim(),contrastCurve:e=>e.platform==="phone"?P(6):P(7)});return U(super.onError(),"2025",t)}errorContainer(){let t=_.fromPalette({name:"error_container",palette:e=>e.errorPalette,tone:e=>e.platform==="watch"?30:e.isDark?zr(e.errorPalette,30,93):ce(e.errorPalette,0,90),isBackground:!0,background:e=>e.platform==="phone"?this.highestSurface(e):void 0,toneDeltaPair:e=>e.platform==="watch"?new xe(this.errorContainer(),this.errorDim(),10,"darker",!0,"farther"):void 0,contrastCurve:e=>e.platform==="phone"&&e.contrastLevel>0?P(1.5):void 0});return U(super.errorContainer(),"2025",t)}onErrorContainer(){let t=_.fromPalette({name:"on_error_container",palette:e=>e.errorPalette,background:e=>this.errorContainer(),contrastCurve:e=>e.platform==="phone"?P(4.5):P(7)});return U(super.onErrorContainer(),"2025",t)}surfaceVariant(){let t=Object.assign(this.surfaceContainerHighest().clone(),{name:"surface_variant"});return U(super.surfaceVariant(),"2025",t)}surfaceTint(){let t=Object.assign(this.primary().clone(),{name:"surface_tint"});return U(super.surfaceTint(),"2025",t)}background(){let t=Object.assign(this.surface().clone(),{name:"background"});return U(super.background(),"2025",t)}onBackground(){let t=Object.assign(this.onSurface().clone(),{name:"on_background",tone:e=>e.platform==="watch"?100:this.onSurface().getTone(e)});return U(super.onBackground(),"2025",t)}};var v=class n{constructor(){this.allColors=[this.background(),this.onBackground(),this.surface(),this.surfaceDim(),this.surfaceBright(),this.surfaceContainerLowest(),this.surfaceContainerLow(),this.surfaceContainer(),this.surfaceContainerHigh(),this.surfaceContainerHighest(),this.onSurface(),this.onSurfaceVariant(),this.outline(),this.outlineVariant(),this.inverseSurface(),this.inverseOnSurface(),this.primary(),this.primaryDim(),this.onPrimary(),this.primaryContainer(),this.onPrimaryContainer(),this.primaryFixed(),this.primaryFixedDim(),this.onPrimaryFixed(),this.onPrimaryFixedVariant(),this.inversePrimary(),this.secondary(),this.secondaryDim(),this.onSecondary(),this.secondaryContainer(),this.onSecondaryContainer(),this.secondaryFixed(),this.secondaryFixedDim(),this.onSecondaryFixed(),this.onSecondaryFixedVariant(),this.tertiary(),this.tertiaryDim(),this.onTertiary(),this.tertiaryContainer(),this.onTertiaryContainer(),this.tertiaryFixed(),this.tertiaryFixedDim(),this.onTertiaryFixed(),this.onTertiaryFixedVariant(),this.error(),this.errorDim(),this.onError(),this.errorContainer(),this.onErrorContainer()].filter(t=>t!==void 0)}highestSurface(t){return n.colorSpec.highestSurface(t)}primaryPaletteKeyColor(){return n.colorSpec.primaryPaletteKeyColor()}secondaryPaletteKeyColor(){return n.colorSpec.secondaryPaletteKeyColor()}tertiaryPaletteKeyColor(){return n.colorSpec.tertiaryPaletteKeyColor()}neutralPaletteKeyColor(){return n.colorSpec.neutralPaletteKeyColor()}neutralVariantPaletteKeyColor(){return n.colorSpec.neutralVariantPaletteKeyColor()}errorPaletteKeyColor(){return n.colorSpec.errorPaletteKeyColor()}background(){return n.colorSpec.background()}onBackground(){return n.colorSpec.onBackground()}surface(){return n.colorSpec.surface()}surfaceDim(){return n.colorSpec.surfaceDim()}surfaceBright(){return n.colorSpec.surfaceBright()}surfaceContainerLowest(){return n.colorSpec.surfaceContainerLowest()}surfaceContainerLow(){return n.colorSpec.surfaceContainerLow()}surfaceContainer(){return n.colorSpec.surfaceContainer()}surfaceContainerHigh(){return n.colorSpec.surfaceContainerHigh()}surfaceContainerHighest(){return n.colorSpec.surfaceContainerHighest()}onSurface(){return n.colorSpec.onSurface()}surfaceVariant(){return n.colorSpec.surfaceVariant()}onSurfaceVariant(){return n.colorSpec.onSurfaceVariant()}outline(){return n.colorSpec.outline()}outlineVariant(){return n.colorSpec.outlineVariant()}inverseSurface(){return n.colorSpec.inverseSurface()}inverseOnSurface(){return n.colorSpec.inverseOnSurface()}shadow(){return n.colorSpec.shadow()}scrim(){return n.colorSpec.scrim()}surfaceTint(){return n.colorSpec.surfaceTint()}primary(){return n.colorSpec.primary()}primaryDim(){return n.colorSpec.primaryDim()}onPrimary(){return n.colorSpec.onPrimary()}primaryContainer(){return n.colorSpec.primaryContainer()}onPrimaryContainer(){return n.colorSpec.onPrimaryContainer()}inversePrimary(){return n.colorSpec.inversePrimary()}primaryFixed(){return n.colorSpec.primaryFixed()}primaryFixedDim(){return n.colorSpec.primaryFixedDim()}onPrimaryFixed(){return n.colorSpec.onPrimaryFixed()}onPrimaryFixedVariant(){return n.colorSpec.onPrimaryFixedVariant()}secondary(){return n.colorSpec.secondary()}secondaryDim(){return n.colorSpec.secondaryDim()}onSecondary(){return n.colorSpec.onSecondary()}secondaryContainer(){return n.colorSpec.secondaryContainer()}onSecondaryContainer(){return n.colorSpec.onSecondaryContainer()}secondaryFixed(){return n.colorSpec.secondaryFixed()}secondaryFixedDim(){return n.colorSpec.secondaryFixedDim()}onSecondaryFixed(){return n.colorSpec.onSecondaryFixed()}onSecondaryFixedVariant(){return n.colorSpec.onSecondaryFixedVariant()}tertiary(){return n.colorSpec.tertiary()}tertiaryDim(){return n.colorSpec.tertiaryDim()}onTertiary(){return n.colorSpec.onTertiary()}tertiaryContainer(){return n.colorSpec.tertiaryContainer()}onTertiaryContainer(){return n.colorSpec.onTertiaryContainer()}tertiaryFixed(){return n.colorSpec.tertiaryFixed()}tertiaryFixedDim(){return n.colorSpec.tertiaryFixedDim()}onTertiaryFixed(){return n.colorSpec.onTertiaryFixed()}onTertiaryFixedVariant(){return n.colorSpec.onTertiaryFixedVariant()}error(){return n.colorSpec.error()}errorDim(){return n.colorSpec.errorDim()}onError(){return n.colorSpec.onError()}errorContainer(){return n.colorSpec.errorContainer()}onErrorContainer(){return n.colorSpec.onErrorContainer()}static highestSurface(t){return n.colorSpec.highestSurface(t)}};v.contentAccentToneDelta=15;v.colorSpec=new Ud;v.primaryPaletteKeyColor=v.colorSpec.primaryPaletteKeyColor();v.secondaryPaletteKeyColor=v.colorSpec.secondaryPaletteKeyColor();v.tertiaryPaletteKeyColor=v.colorSpec.tertiaryPaletteKeyColor();v.neutralPaletteKeyColor=v.colorSpec.neutralPaletteKeyColor();v.neutralVariantPaletteKeyColor=v.colorSpec.neutralVariantPaletteKeyColor();v.background=v.colorSpec.background();v.onBackground=v.colorSpec.onBackground();v.surface=v.colorSpec.surface();v.surfaceDim=v.colorSpec.surfaceDim();v.surfaceBright=v.colorSpec.surfaceBright();v.surfaceContainerLowest=v.colorSpec.surfaceContainerLowest();v.surfaceContainerLow=v.colorSpec.surfaceContainerLow();v.surfaceContainer=v.colorSpec.surfaceContainer();v.surfaceContainerHigh=v.colorSpec.surfaceContainerHigh();v.surfaceContainerHighest=v.colorSpec.surfaceContainerHighest();v.onSurface=v.colorSpec.onSurface();v.surfaceVariant=v.colorSpec.surfaceVariant();v.onSurfaceVariant=v.colorSpec.onSurfaceVariant();v.inverseSurface=v.colorSpec.inverseSurface();v.inverseOnSurface=v.colorSpec.inverseOnSurface();v.outline=v.colorSpec.outline();v.outlineVariant=v.colorSpec.outlineVariant();v.shadow=v.colorSpec.shadow();v.scrim=v.colorSpec.scrim();v.surfaceTint=v.colorSpec.surfaceTint();v.primary=v.colorSpec.primary();v.onPrimary=v.colorSpec.onPrimary();v.primaryContainer=v.colorSpec.primaryContainer();v.onPrimaryContainer=v.colorSpec.onPrimaryContainer();v.inversePrimary=v.colorSpec.inversePrimary();v.secondary=v.colorSpec.secondary();v.onSecondary=v.colorSpec.onSecondary();v.secondaryContainer=v.colorSpec.secondaryContainer();v.onSecondaryContainer=v.colorSpec.onSecondaryContainer();v.tertiary=v.colorSpec.tertiary();v.onTertiary=v.colorSpec.onTertiary();v.tertiaryContainer=v.colorSpec.tertiaryContainer();v.onTertiaryContainer=v.colorSpec.onTertiaryContainer();v.error=v.colorSpec.error();v.onError=v.colorSpec.onError();v.errorContainer=v.colorSpec.errorContainer();v.onErrorContainer=v.colorSpec.onErrorContainer();v.primaryFixed=v.colorSpec.primaryFixed();v.primaryFixedDim=v.colorSpec.primaryFixedDim();v.onPrimaryFixed=v.colorSpec.onPrimaryFixed();v.onPrimaryFixedVariant=v.colorSpec.onPrimaryFixedVariant();v.secondaryFixed=v.colorSpec.secondaryFixed();v.secondaryFixedDim=v.colorSpec.secondaryFixedDim();v.onSecondaryFixed=v.colorSpec.onSecondaryFixed();v.onSecondaryFixedVariant=v.colorSpec.onSecondaryFixedVariant();v.tertiaryFixed=v.colorSpec.tertiaryFixed();v.tertiaryFixedDim=v.colorSpec.tertiaryFixedDim();v.onTertiaryFixed=v.colorSpec.onTertiaryFixed();v.onTertiaryFixedVariant=v.colorSpec.onTertiaryFixedVariant();var Ae=(()=>{class n{static maybeFallbackSpecVersion(e,r){switch(r){case b.EXPRESSIVE:case b.VIBRANT:case b.TONAL_SPOT:case b.NEUTRAL:return e;default:return"2021"}}constructor(e){this.sourceColorArgb=e.sourceColorHct.toInt(),this.variant=e.variant,this.contrastLevel=e.contrastLevel,this.isDark=e.isDark,this.platform=e.platform??"phone",this.specVersion=n.maybeFallbackSpecVersion(e.specVersion??"2021",this.variant),this.sourceColorHct=e.sourceColorHct,this.primaryPalette=e.primaryPalette??qo(this.specVersion).getPrimaryPalette(this.variant,e.sourceColorHct,this.isDark,this.platform,this.contrastLevel),this.secondaryPalette=e.secondaryPalette??qo(this.specVersion).getSecondaryPalette(this.variant,e.sourceColorHct,this.isDark,this.platform,this.contrastLevel),this.tertiaryPalette=e.tertiaryPalette??qo(this.specVersion).getTertiaryPalette(this.variant,e.sourceColorHct,this.isDark,this.platform,this.contrastLevel),this.neutralPalette=e.neutralPalette??qo(this.specVersion).getNeutralPalette(this.variant,e.sourceColorHct,this.isDark,this.platform,this.contrastLevel),this.neutralVariantPalette=e.neutralVariantPalette??qo(this.specVersion).getNeutralVariantPalette(this.variant,e.sourceColorHct,this.isDark,this.platform,this.contrastLevel),this.errorPalette=e.errorPalette??qo(this.specVersion).getErrorPalette(this.variant,e.sourceColorHct,this.isDark,this.platform,this.contrastLevel)??k.fromHueAndChroma(25,84),this.colors=new v}toString(){return`Scheme: variant=${b[this.variant]}, mode=${this.isDark?"dark":"light"}, platform=${this.platform}, contrastLevel=${this.contrastLevel.toFixed(1)}, seed=${this.sourceColorHct.toString()}, specVersion=${this.specVersion}`}static getPiecewiseHue(e,r,i){let o=Math.min(r.length-1,i.length),s=e.hue;for(let a=0;a<o;a++)if(s>=r[a]&&s<r[a+1])return dt(i[a]);return s}static getRotatedHue(e,r,i){let o=n.getPiecewiseHue(e,r,i);return Math.min(r.length-1,i.length)<=0&&(o=0),dt(e.hue+o)}getArgb(e){return e.getArgb(this)}getHct(e){return e.getHct(this)}get primaryPaletteKeyColor(){return this.getArgb(this.colors.primaryPaletteKeyColor())}get secondaryPaletteKeyColor(){return this.getArgb(this.colors.secondaryPaletteKeyColor())}get tertiaryPaletteKeyColor(){return this.getArgb(this.colors.tertiaryPaletteKeyColor())}get neutralPaletteKeyColor(){return this.getArgb(this.colors.neutralPaletteKeyColor())}get neutralVariantPaletteKeyColor(){return this.getArgb(this.colors.neutralVariantPaletteKeyColor())}get errorPaletteKeyColor(){return this.getArgb(this.colors.errorPaletteKeyColor())}get background(){return this.getArgb(this.colors.background())}get onBackground(){return this.getArgb(this.colors.onBackground())}get surface(){return this.getArgb(this.colors.surface())}get surfaceDim(){return this.getArgb(this.colors.surfaceDim())}get surfaceBright(){return this.getArgb(this.colors.surfaceBright())}get surfaceContainerLowest(){return this.getArgb(this.colors.surfaceContainerLowest())}get surfaceContainerLow(){return this.getArgb(this.colors.surfaceContainerLow())}get surfaceContainer(){return this.getArgb(this.colors.surfaceContainer())}get surfaceContainerHigh(){return this.getArgb(this.colors.surfaceContainerHigh())}get surfaceContainerHighest(){return this.getArgb(this.colors.surfaceContainerHighest())}get onSurface(){return this.getArgb(this.colors.onSurface())}get surfaceVariant(){return this.getArgb(this.colors.surfaceVariant())}get onSurfaceVariant(){return this.getArgb(this.colors.onSurfaceVariant())}get inverseSurface(){return this.getArgb(this.colors.inverseSurface())}get inverseOnSurface(){return this.getArgb(this.colors.inverseOnSurface())}get outline(){return this.getArgb(this.colors.outline())}get outlineVariant(){return this.getArgb(this.colors.outlineVariant())}get shadow(){return this.getArgb(this.colors.shadow())}get scrim(){return this.getArgb(this.colors.scrim())}get surfaceTint(){return this.getArgb(this.colors.surfaceTint())}get primary(){return this.getArgb(this.colors.primary())}get primaryDim(){let e=this.colors.primaryDim();if(e===void 0)throw new Error("`primaryDim` color is undefined prior to 2025 spec.");return this.getArgb(e)}get onPrimary(){return this.getArgb(this.colors.onPrimary())}get primaryContainer(){return this.getArgb(this.colors.primaryContainer())}get onPrimaryContainer(){return this.getArgb(this.colors.onPrimaryContainer())}get primaryFixed(){return this.getArgb(this.colors.primaryFixed())}get primaryFixedDim(){return this.getArgb(this.colors.primaryFixedDim())}get onPrimaryFixed(){return this.getArgb(this.colors.onPrimaryFixed())}get onPrimaryFixedVariant(){return this.getArgb(this.colors.onPrimaryFixedVariant())}get inversePrimary(){return this.getArgb(this.colors.inversePrimary())}get secondary(){return this.getArgb(this.colors.secondary())}get secondaryDim(){let e=this.colors.secondaryDim();if(e===void 0)throw new Error("`secondaryDim` color is undefined prior to 2025 spec.");return this.getArgb(e)}get onSecondary(){return this.getArgb(this.colors.onSecondary())}get secondaryContainer(){return this.getArgb(this.colors.secondaryContainer())}get onSecondaryContainer(){return this.getArgb(this.colors.onSecondaryContainer())}get secondaryFixed(){return this.getArgb(this.colors.secondaryFixed())}get secondaryFixedDim(){return this.getArgb(this.colors.secondaryFixedDim())}get onSecondaryFixed(){return this.getArgb(this.colors.onSecondaryFixed())}get onSecondaryFixedVariant(){return this.getArgb(this.colors.onSecondaryFixedVariant())}get tertiary(){return this.getArgb(this.colors.tertiary())}get tertiaryDim(){let e=this.colors.tertiaryDim();if(e===void 0)throw new Error("`tertiaryDim` color is undefined prior to 2025 spec.");return this.getArgb(e)}get onTertiary(){return this.getArgb(this.colors.onTertiary())}get tertiaryContainer(){return this.getArgb(this.colors.tertiaryContainer())}get onTertiaryContainer(){return this.getArgb(this.colors.onTertiaryContainer())}get tertiaryFixed(){return this.getArgb(this.colors.tertiaryFixed())}get tertiaryFixedDim(){return this.getArgb(this.colors.tertiaryFixedDim())}get onTertiaryFixed(){return this.getArgb(this.colors.onTertiaryFixed())}get onTertiaryFixedVariant(){return this.getArgb(this.colors.onTertiaryFixedVariant())}get error(){return this.getArgb(this.colors.error())}get errorDim(){let e=this.colors.errorDim();if(e===void 0)throw new Error("`errorDim` color is undefined prior to 2025 spec.");return this.getArgb(e)}get onError(){return this.getArgb(this.colors.onError())}get errorContainer(){return this.getArgb(this.colors.errorContainer())}get onErrorContainer(){return this.getArgb(this.colors.onErrorContainer())}}return n.DEFAULT_SPEC_VERSION="2021",n.DEFAULT_PLATFORM="phone",n})(),$d=class{getPrimaryPalette(t,e,r,i,o){switch(t){case b.CONTENT:case b.FIDELITY:return k.fromHueAndChroma(e.hue,e.chroma);case b.FRUIT_SALAD:return k.fromHueAndChroma(dt(e.hue-50),48);case b.MONOCHROME:return k.fromHueAndChroma(e.hue,0);case b.NEUTRAL:return k.fromHueAndChroma(e.hue,12);case b.RAINBOW:return k.fromHueAndChroma(e.hue,48);case b.TONAL_SPOT:return k.fromHueAndChroma(e.hue,36);case b.EXPRESSIVE:return k.fromHueAndChroma(dt(e.hue+240),40);case b.VIBRANT:return k.fromHueAndChroma(e.hue,200);default:throw new Error(`Unsupported variant: ${t}`)}}getSecondaryPalette(t,e,r,i,o){switch(t){case b.CONTENT:case b.FIDELITY:return k.fromHueAndChroma(e.hue,Math.max(e.chroma-32,e.chroma*.5));case b.FRUIT_SALAD:return k.fromHueAndChroma(dt(e.hue-50),36);case b.MONOCHROME:return k.fromHueAndChroma(e.hue,0);case b.NEUTRAL:return k.fromHueAndChroma(e.hue,8);case b.RAINBOW:return k.fromHueAndChroma(e.hue,16);case b.TONAL_SPOT:return k.fromHueAndChroma(e.hue,16);case b.EXPRESSIVE:return k.fromHueAndChroma(Ae.getRotatedHue(e,[0,21,51,121,151,191,271,321,360],[45,95,45,20,45,90,45,45,45]),24);case b.VIBRANT:return k.fromHueAndChroma(Ae.getRotatedHue(e,[0,41,61,101,131,181,251,301,360],[18,15,10,12,15,18,15,12,12]),24);default:throw new Error(`Unsupported variant: ${t}`)}}getTertiaryPalette(t,e,r,i,o){switch(t){case b.CONTENT:return k.fromHct(Yi.fixIfDisliked(new Ia(e).analogous(3,6)[2]));case b.FIDELITY:return k.fromHct(Yi.fixIfDisliked(new Ia(e).complement));case b.FRUIT_SALAD:return k.fromHueAndChroma(e.hue,36);case b.MONOCHROME:return k.fromHueAndChroma(e.hue,0);case b.NEUTRAL:return k.fromHueAndChroma(e.hue,16);case b.RAINBOW:case b.TONAL_SPOT:return k.fromHueAndChroma(dt(e.hue+60),24);case b.EXPRESSIVE:return k.fromHueAndChroma(Ae.getRotatedHue(e,[0,21,51,121,151,191,271,321,360],[120,120,20,45,20,15,20,120,120]),32);case b.VIBRANT:return k.fromHueAndChroma(Ae.getRotatedHue(e,[0,41,61,101,131,181,251,301,360],[35,30,20,25,30,35,30,25,25]),32);default:throw new Error(`Unsupported variant: ${t}`)}}getNeutralPalette(t,e,r,i,o){switch(t){case b.CONTENT:case b.FIDELITY:return k.fromHueAndChroma(e.hue,e.chroma/8);case b.FRUIT_SALAD:return k.fromHueAndChroma(e.hue,10);case b.MONOCHROME:return k.fromHueAndChroma(e.hue,0);case b.NEUTRAL:return k.fromHueAndChroma(e.hue,2);case b.RAINBOW:return k.fromHueAndChroma(e.hue,0);case b.TONAL_SPOT:return k.fromHueAndChroma(e.hue,6);case b.EXPRESSIVE:return k.fromHueAndChroma(dt(e.hue+15),8);case b.VIBRANT:return k.fromHueAndChroma(e.hue,10);default:throw new Error(`Unsupported variant: ${t}`)}}getNeutralVariantPalette(t,e,r,i,o){switch(t){case b.CONTENT:return k.fromHueAndChroma(e.hue,e.chroma/8+4);case b.FIDELITY:return k.fromHueAndChroma(e.hue,e.chroma/8+4);case b.FRUIT_SALAD:return k.fromHueAndChroma(e.hue,16);case b.MONOCHROME:return k.fromHueAndChroma(e.hue,0);case b.NEUTRAL:return k.fromHueAndChroma(e.hue,2);case b.RAINBOW:return k.fromHueAndChroma(e.hue,0);case b.TONAL_SPOT:return k.fromHueAndChroma(e.hue,8);case b.EXPRESSIVE:return k.fromHueAndChroma(dt(e.hue+15),12);case b.VIBRANT:return k.fromHueAndChroma(e.hue,12);default:throw new Error(`Unsupported variant: ${t}`)}}getErrorPalette(t,e,r,i,o){}},tg=class n extends $d{getPrimaryPalette(t,e,r,i,o){switch(t){case b.NEUTRAL:return k.fromHueAndChroma(e.hue,i==="phone"?O.isBlue(e.hue)?12:8:O.isBlue(e.hue)?16:12);case b.TONAL_SPOT:return k.fromHueAndChroma(e.hue,i==="phone"&&r?26:32);case b.EXPRESSIVE:return k.fromHueAndChroma(e.hue,i==="phone"?r?36:48:40);case b.VIBRANT:return k.fromHueAndChroma(e.hue,i==="phone"?74:56);default:return super.getPrimaryPalette(t,e,r,i,o)}}getSecondaryPalette(t,e,r,i,o){switch(t){case b.NEUTRAL:return k.fromHueAndChroma(e.hue,i==="phone"?O.isBlue(e.hue)?6:4:O.isBlue(e.hue)?10:6);case b.TONAL_SPOT:return k.fromHueAndChroma(e.hue,16);case b.EXPRESSIVE:return k.fromHueAndChroma(Ae.getRotatedHue(e,[0,105,140,204,253,278,300,333,360],[-160,155,-100,96,-96,-156,-165,-160]),i==="phone"&&r?16:24);case b.VIBRANT:return k.fromHueAndChroma(Ae.getRotatedHue(e,[0,38,105,140,333,360],[-14,10,-14,10,-14]),i==="phone"?56:36);default:return super.getSecondaryPalette(t,e,r,i,o)}}getTertiaryPalette(t,e,r,i,o){switch(t){case b.NEUTRAL:return k.fromHueAndChroma(Ae.getRotatedHue(e,[0,38,105,161,204,278,333,360],[-32,26,10,-39,24,-15,-32]),i==="phone"?20:36);case b.TONAL_SPOT:return k.fromHueAndChroma(Ae.getRotatedHue(e,[0,20,71,161,333,360],[-40,48,-32,40,-32]),i==="phone"?28:32);case b.EXPRESSIVE:return k.fromHueAndChroma(Ae.getRotatedHue(e,[0,105,140,204,253,278,300,333,360],[-165,160,-105,101,-101,-160,-170,-165]),48);case b.VIBRANT:return k.fromHueAndChroma(Ae.getRotatedHue(e,[0,38,71,105,140,161,253,333,360],[-72,35,24,-24,62,50,62,-72]),56);default:return super.getTertiaryPalette(t,e,r,i,o)}}static getExpressiveNeutralHue(t){return Ae.getRotatedHue(t,[0,71,124,253,278,300,360],[10,0,10,0,10,0])}static getExpressiveNeutralChroma(t,e,r){let i=n.getExpressiveNeutralHue(t);return r==="phone"?e?O.isYellow(i)?6:14:18:12}static getVibrantNeutralHue(t){return Ae.getRotatedHue(t,[0,38,105,140,333,360],[-14,10,-14,10,-14])}static getVibrantNeutralChroma(t,e){let r=n.getVibrantNeutralHue(t);return e==="phone"||O.isBlue(r)?28:20}getNeutralPalette(t,e,r,i,o){switch(t){case b.NEUTRAL:return k.fromHueAndChroma(e.hue,i==="phone"?1.4:6);case b.TONAL_SPOT:return k.fromHueAndChroma(e.hue,i==="phone"?5:10);case b.EXPRESSIVE:return k.fromHueAndChroma(n.getExpressiveNeutralHue(e),n.getExpressiveNeutralChroma(e,r,i));case b.VIBRANT:return k.fromHueAndChroma(n.getVibrantNeutralHue(e),n.getVibrantNeutralChroma(e,i));default:return super.getNeutralPalette(t,e,r,i,o)}}getNeutralVariantPalette(t,e,r,i,o){switch(t){case b.NEUTRAL:return k.fromHueAndChroma(e.hue,(i==="phone"?1.4:6)*2.2);case b.TONAL_SPOT:return k.fromHueAndChroma(e.hue,(i==="phone"?5:10)*1.7);case b.EXPRESSIVE:let s=n.getExpressiveNeutralHue(e),a=n.getExpressiveNeutralChroma(e,r,i);return k.fromHueAndChroma(s,a*(s>=105&&s<125?1.6:2.3));case b.VIBRANT:let c=n.getVibrantNeutralHue(e),l=n.getVibrantNeutralChroma(e,i);return k.fromHueAndChroma(c,l*1.29);default:return super.getNeutralVariantPalette(t,e,r,i,o)}}getErrorPalette(t,e,r,i,o){let s=Ae.getPiecewiseHue(e,[0,3,13,23,33,43,153,273,360],[12,22,32,12,22,32,22,12]);switch(t){case b.NEUTRAL:return k.fromHueAndChroma(s,i==="phone"?50:40);case b.TONAL_SPOT:return k.fromHueAndChroma(s,i==="phone"?60:48);case b.EXPRESSIVE:return k.fromHueAndChroma(s,i==="phone"?64:48);case b.VIBRANT:return k.fromHueAndChroma(s,i==="phone"?80:60);default:return super.getErrorPalette(t,e,r,i,o)}}},cO=new $d,lO=new tg;function qo(n){return n==="2025"?lO:cO}function Gd(n){let t=Da(n),e=wa(n),r=Ca(n),i=[t.toString(16),e.toString(16),r.toString(16)];for(let[o,s]of i.entries())s.length===1&&(i[o]="0"+s);return"#"+i.join("")}function zD(n){n=n.replace("#","");let t=n.length===3,e=n.length===6,r=n.length===8;if(!t&&!e&&!r)throw new Error("unexpected hex "+n);let i=0,o=0,s=0;return t?(i=fr(n.slice(0,1).repeat(2)),o=fr(n.slice(1,2).repeat(2)),s=fr(n.slice(2,3).repeat(2))):e?(i=fr(n.slice(0,2)),o=fr(n.slice(2,4)),s=fr(n.slice(4,6))):r&&(i=fr(n.slice(2,4)),o=fr(n.slice(4,6)),s=fr(n.slice(6,8))),(255<<24|(i&255)<<16|(o&255)<<8|s&255)>>>0}function fr(n){return parseInt(n,16)}var UD={contrastLevel:5,variant:b.VIBRANT,platform:"phone",specVersion:"2025"},Wd=class n{appDocument=f(B);stylesheet=new CSSStyleSheet;sourceColor=X(null);hct=Me(()=>{let t=this.sourceColor();return t?O.fromInt(zD(t)):null});updateStylesheet=bt(()=>{if(!this.sourceColor()){this.stylesheet.replaceSync("");return}let t=new Ae(W(w({},UD),{sourceColorHct:this.hct(),isDark:!1})),e=new Ae(W(w({},UD),{sourceColorHct:this.hct(),isDark:!0})),r=this.assembleCSSContent(t,e);this.stylesheet.replaceSync(r)});constructor(){this.appDocument.adoptedStyleSheets.push(this.stylesheet)}camelToKebab(t){return t.replace(/[A-Z]+(?![a-z])|[A-Z]/g,(e,r)=>(r?"-":"")+e.toLowerCase())}assembleCSSContent(t,e){let r=Object.entries(dO).reduce((i,[o,s])=>{let{get:a,set:c=void 0,writable:l=!1}=s;return a&&(i[`--mat-sys-${this.camelToKebab(o)}`]=`light-dark(${Gd(a.apply(t))}, ${Gd(a.apply(e))})`),i},{});return`:root { ${Object.entries(r).map(([i,o])=>`${i}: ${o};`).join(`
`)} }`}updateColor(t){this.sourceColor.set(t)}static \u0275fac=function(e){return new(e||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})},dO=Object.getOwnPropertyDescriptors(Ae.prototype);function uO(n,t){}var Ur=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var rg=(()=>{class n extends Br{_elementRef=f(z);_focusTrapFactory=f(Gh);_config;_interactivityChecker=f($h);_ngZone=f(F);_focusMonitor=f(Pi);_renderer=f(lt);_changeDetectorRef=f(tt);_injector=f(V);_platform=f(Te);_document=f(B);_portalOutlet;_focusTrapped=new E;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=f(Ur,{optional:!0})||new Ur,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let r=this._ariaLabelledByQueue.indexOf(e);r>-1&&(this._ariaLabelledByQueue.splice(r,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let r=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),r}attachTemplatePortal(e){this._portalOutlet.hasAttached();let r=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),r}attachDomPortal=e=>{this._portalOutlet.hasAttached();let r=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),r};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,r){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let i=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",i),s=this._renderer.listen(e,"mousedown",i)})),e.focus(r)}_focusByCssSelector(e,r){let i=this._elementRef.nativeElement.querySelector(e);i&&this._forceFocus(i,r)}_trapFocus(e){this._isDestroyed||Ft(()=>{let r=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||r.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,r=null;if(typeof e=="string"?r=this._document.querySelector(e):typeof e=="boolean"?r=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(r=e),this._config.restoreFocus&&r&&typeof r.focus=="function"){let i=Ys(),o=this._elementRef.nativeElement;(!i||i===this._document.body||i===o||o.contains(i))&&(this._focusMonitor?(this._focusMonitor.focusVia(r,this._closeInteractionType),this._closeInteractionType=null):r.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,r=Ys();return e===r||e.contains(r)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Ys()))}static \u0275fac=function(r){return new(r||n)};static \u0275cmp=q({type:n,selectors:[["cdk-dialog-container"]],viewQuery:function(r,i){if(r&1&&et(Un,7),r&2){let o;J(o=ee())&&(i._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(r,i){r&2&&ue("id",i._config.id||null)("role",i._config.role)("aria-modal",i._config.ariaModal)("aria-labelledby",i._config.ariaLabel?null:i._ariaLabelledByQueue[0])("aria-label",i._config.ariaLabel)("aria-describedby",i._config.ariaDescribedBy||null)},features:[Pe],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(r,i){r&1&&Ue(0,uO,0,0,"ng-template",0)},dependencies:[Un],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return n})(),Sa=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new E;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(t,e){this.overlayRef=t,this.config=e,this.disableClose=e.disableClose,this.backdropClick=t.backdropClick(),this.keydownEvents=t.keydownEvents(),this.outsidePointerEvents=t.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(r=>{r.keyCode===27&&!this.disableClose&&!xt(r)&&(r.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=t.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(t,e){if(this._canClose(t)){let r=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),r.next(t),r.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(t="",e=""){return this.overlayRef.updateSize({width:t,height:e}),this}addPanelClass(t){return this.overlayRef.addPanelClass(t),this}removePanelClass(t){return this.overlayRef.removePanelClass(t),this}_canClose(t){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(t,e,this.componentInstance))}},fO=new x("DialogScrollStrategy",{providedIn:"root",factory:()=>{let n=f(V);return()=>ia(n)}}),mO=new x("DialogData"),hO=new x("DefaultDialogConfig");function pO(n){let t=X(n),e=new j;return{valueSignal:t,get value(){return t()},change:e,ngOnDestroy(){e.complete()}}}var $D=(()=>{class n{_injector=f(V);_defaultOptions=f(hO,{optional:!0});_parentDialog=f(n,{optional:!0,skipSelf:!0});_overlayContainer=f(ad);_idGenerator=f(Fe);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new E;_afterOpenedAtThisLevel=new E;_ariaHiddenElements=new Map;_scrollStrategy=f(fO);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=ei(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(pt(void 0)));constructor(){}open(e,r){let i=this._defaultOptions||new Ur;r=w(w({},i),r),r.id=r.id||this._idGenerator.getId("cdk-dialog-"),r.id&&this.getDialogById(r.id);let o=this._getOverlayConfig(r),s=Bo(this._injector,o),a=new Sa(s,r),c=this._attachContainer(s,a,r);if(a.containerInstance=c,!this.openDialogs.length){let l=this._overlayContainer.getContainerElement();c._focusTrapped?c._focusTrapped.pipe(Mt(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(l)}):this._hideNonDialogContentFromAssistiveTechnology(l)}return this._attachDialogContent(e,a,c,r),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){ng(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(r=>r.id===e)}ngOnDestroy(){ng(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),ng(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let r=new ur({positionStrategy:e.positionStrategy||Vi().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(r.backdropClass=e.backdropClass),r}_attachContainer(e,r,i){let o=i.injector||i.viewContainerRef?.injector,s=[{provide:Ur,useValue:i},{provide:Sa,useValue:r},{provide:Fo,useValue:e}],a;i.container?typeof i.container=="function"?a=i.container:(a=i.container.type,s.push(...i.container.providers(i))):a=rg;let c=new dr(a,i.viewContainerRef,V.create({parent:o||this._injector,providers:s}));return e.attach(c).instance}_attachDialogContent(e,r,i,o){if(e instanceof yt){let s=this._createInjector(o,r,i,void 0),a={$implicit:o.data,dialogRef:r};o.templateContext&&(a=w(w({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),i.attachTemplatePortal(new Cn(e,null,a,s))}else{let s=this._createInjector(o,r,i,this._injector),a=i.attachComponentPortal(new dr(e,o.viewContainerRef,s));r.componentRef=a,r.componentInstance=a.instance}}_createInjector(e,r,i,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:mO,useValue:e.data},{provide:Sa,useValue:r}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(r,e,i)):a.push(...e.providers)),e.direction&&(!s||!s.get(wn,null,{optional:!0}))&&a.push({provide:wn,useValue:pO(e.direction)}),V.create({parent:s||o,providers:a})}_removeOpenDialog(e,r){let i=this.openDialogs.indexOf(e);i>-1&&(this.openDialogs.splice(i,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),r&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let r=e.parentElement.children;for(let i=r.length-1;i>-1;i--){let o=r[i];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function ng(n,t){let e=n.length;for(;e--;)t(n[e])}function gO(n,t){}var Yd=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},ig="mdc-dialog--open",GD="mdc-dialog--opening",WD="mdc-dialog--closing",bO=150,yO=75,_O=(()=>{class n extends rg{_animationStateChanged=new j;_animationsEnabled=!Ge();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?YD(this._config.enterAnimationDuration)??bO:0;_exitAnimationDuration=this._animationsEnabled?YD(this._config.exitAnimationDuration)??yO:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(qD,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(GD,ig)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(ig),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(ig),this._animationsEnabled?(this._hostElement.style.setProperty(qD,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(WD)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(GD,WD)}_waitForAnimationToComplete(e,r){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(r,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let r=super.attachComponentPortal(e);return r.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),r}static \u0275fac=(()=>{let e;return function(i){return(e||(e=Kt(n)))(i||n)}})();static \u0275cmp=q({type:n,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(r,i){r&2&&(Hn("id",i._config.id),ue("aria-modal",i._config.ariaModal)("role",i._config.role)("aria-labelledby",i._config.ariaLabel?null:i._ariaLabelledByQueue[0])("aria-label",i._config.ariaLabel)("aria-describedby",i._config.ariaDescribedBy||null),Q("_mat-animation-noopable",!i._animationsEnabled)("mat-mdc-dialog-container-with-actions",i._actionSectionCount>0))},features:[Pe],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(r,i){r&1&&(I(0,"div",0)(1,"div",1),Ue(2,gO,0,0,"ng-template",2),T()())},dependencies:[Un],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--mat-dialog-container-max-width, 560px);
  min-width: var(--mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--mat-dialog-container-elevation-shadow, none);
  border-radius: var(--mat-dialog-container-shape, var(--mat-sys-corner-extra-large, 4px));
  background-color: var(--mat-dialog-container-color, var(--mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--mat-dialog-subhead-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-dialog-subhead-font, var(--mat-sys-headline-small-font, inherit));
  line-height: var(--mat-dialog-subhead-line-height, var(--mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--mat-dialog-subhead-size, var(--mat-sys-headline-small-size, 1rem));
  font-weight: var(--mat-dialog-subhead-weight, var(--mat-sys-headline-small-weight, 400));
  letter-spacing: var(--mat-dialog-subhead-tracking, var(--mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--mat-dialog-supporting-text-color, var(--mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--mat-dialog-supporting-text-font, var(--mat-sys-body-medium-font, inherit));
  line-height: var(--mat-dialog-supporting-text-line-height, var(--mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--mat-dialog-supporting-text-size, var(--mat-sys-body-medium-size, 1rem));
  font-weight: var(--mat-dialog-supporting-text-weight, var(--mat-sys-body-medium-weight, 400));
  letter-spacing: var(--mat-dialog-supporting-text-tracking, var(--mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2})}return n})(),qD="--mat-dialog-transition-duration";function YD(n){return n==null?null:typeof n=="number"?n:n.endsWith("ms")?Ks(n.substring(0,n.length-2)):n.endsWith("s")?Ks(n.substring(0,n.length-1))*1e3:n==="0"?0:null}var qd=(function(n){return n[n.OPEN=0]="OPEN",n[n.CLOSING=1]="CLOSING",n[n.CLOSED=2]="CLOSED",n})(qd||{}),og=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new vr(1);_beforeClosed=new vr(1);_result;_closeFallbackTimeout;_state=qd.OPEN;_closeInteractionType;constructor(t,e,r){this._ref=t,this._config=e,this._containerInstance=r,this.disableClose=e.disableClose,this.id=t.id,t.addPanelClass("mat-mdc-dialog-panel"),r._animationStateChanged.pipe(Ie(i=>i.state==="opened"),Mt(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),r._animationStateChanged.pipe(Ie(i=>i.state==="closed"),Mt(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),t.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),an(this.backdropClick(),this.keydownEvents().pipe(Ie(i=>i.keyCode===27&&!this.disableClose&&!xt(i)))).subscribe(i=>{this.disableClose||(i.preventDefault(),vO(this,i.type==="keydown"?"keyboard":"mouse"))})}close(t){let e=this._config.closePredicate;e&&!e(t,this._config,this.componentInstance)||(this._result=t,this._containerInstance._animationStateChanged.pipe(Ie(r=>r.state==="closing"),Mt(1)).subscribe(r=>{this._beforeClosed.next(t),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),r.totalTime+100)}),this._state=qd.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(t){let e=this._ref.config.positionStrategy;return t&&(t.left||t.right)?t.left?e.left(t.left):e.right(t.right):e.centerHorizontally(),t&&(t.top||t.bottom)?t.top?e.top(t.top):e.bottom(t.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(t="",e=""){return this._ref.updateSize(t,e),this}addPanelClass(t){return this._ref.addPanelClass(t),this}removePanelClass(t){return this._ref.removePanelClass(t),this}getState(){return this._state}_finishDialogClose(){this._state=qd.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function vO(n,t,e){return n._closeInteractionType=t,n.close(e)}var xO=new x("MatMdcDialogData"),DO=new x("mat-mdc-dialog-default-options"),wO=new x("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let n=f(V);return()=>ia(n)}}),ZD=(()=>{class n{_defaultOptions=f(DO,{optional:!0});_scrollStrategy=f(wO);_parentDialog=f(n,{optional:!0,skipSelf:!0});_idGenerator=f(Fe);_injector=f(V);_dialog=f($D);_animationsDisabled=Ge();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new E;_afterOpenedAtThisLevel=new E;dialogConfigClass=Yd;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=ei(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(pt(void 0)));constructor(){this._dialogRefConstructor=og,this._dialogContainerType=_O,this._dialogDataToken=xO}open(e,r){let i;r=w(w({},this._defaultOptions||new Yd),r),r.id=r.id||this._idGenerator.getId("mat-mdc-dialog-"),r.scrollStrategy=r.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,W(w({},r),{positionStrategy:Vi(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||r.enterAnimationDuration?.toLocaleString()==="0"||r.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:r},{provide:Ur,useValue:r}]},templateContext:()=>({dialogRef:i}),providers:(s,a,c)=>(i=new this._dialogRefConstructor(s,r,c),i.updatePosition(r?.position),[{provide:this._dialogContainerType,useValue:c},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:i}])}));return i.componentRef=o.componentRef,i.componentInstance=o.componentInstance,this.openDialogs.push(i),this.afterOpened.next(i),i.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(i);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),i}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(r=>r.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let r=e.length;for(;r--;)e[r].close()}static \u0275fac=function(r){return new(r||n)};static \u0275prov=C({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var CO=["transitionPlayer"],EO=()=>[],KD=n=>({$implicit:n}),kO=(n,t)=>t==null?null:t.id;function IO(n,t){n&1&&(I(0,"div",2),fe(1,"mat-progress-spinner",3),I(2,"h1"),_t(3,"we loadin..."),T()())}function SO(n,t){n&1&&ar(0)}function TO(n,t){if(n&1&&Ue(0,SO,1,0,"ng-container",12),n&2){ne();let e=mh(4);ne();let r=Vt(3);te("ngTemplateOutlet",r)("ngTemplateOutletContext",kl(2,KD,e))}}function MO(n,t){n&1&&ar(0)}function AO(n,t){if(n&1&&(I(0,"mat-option",8),Ue(1,MO,1,0,"ng-container",12),T()),n&2){let e=t.$implicit,r=ne(2),i=Vt(3);te("value",e.id),S(),te("ngTemplateOutlet",i)("ngTemplateOutletContext",kl(3,KD,r.mergeVariantData(e)))}}function RO(n,t){if(n&1&&fe(0,"bossfight-render-component",11),n&2){let e=ne(2).selectedDocumentRecord();te("documentRecord",e)}}function OO(n,t){if(n&1){let e=jn();I(0,"mat-toolbar",4)(1,"mat-form-field",5)(2,"mat-select",6),Cl("ngModelChange",function(i){Yt(e);let o=ne();return uh(o.selectedBossfight,i)||(o.selectedBossfight=i),Zt(i)}),I(3,"mat-select-trigger",7),El(4),pe(5,TO,1,4,"ng-container"),T(),Ii(6,AO,2,5,"mat-option",8,kO),T()(),fe(8,"div",9),T(),I(9,"div",10),pe(10,RO,1,1,"bossfight-render-component",11),T()}if(n&2){let e=ne();S(2),wl("ngModel",e.selectedBossfight),te("value",e.selectedBossfight()),S(2);let r=fh(e.selectedDocumentRecord());S(),ge(r?5:-1),S(),Si(e.bossfightData()??hh(5,EO)),S(4),ge(e.selectedDocumentRecord()?10:-1)}}function NO(n,t){if(n&1&&(I(0,"span",13),fe(1,"img",14),_t(2),T()),n&2){let e=t.$implicit;S(),te("src",e.icon??"images/missing_icon.png",ul),S(),en(" ",e.title," ")}}function PO(n,t){if(n&1){let e=jn();I(0,"video",15),Ce("ended",function(){Yt(e);let i=ne();return Zt(i.transitionEnded())}),T()}if(n&2){let e=t.$implicit;te("src",e,ul)}}var Zd=class n{bossfightDataService=f(Td);userDataService=f($o);appColorService=f(Wd);_snackBar=f(Dx);_dialog=f(ZD);transitionPlayerTemplate=Mi.required("transitionPlayer");dialogRef=X(null);snackBarRef=X(null);userData=this.userDataService.userData;bossfightDataLoading=this.bossfightDataService.isLoading;bossfightData=this.bossfightDataService.data;bossfightDataError=this.bossfightDataService.error;bossfightError=bt(()=>{let t=this.bossfightDataError();t?this.snackBarRef.set(this._snackBar.open(t.message,"Reload")):this._snackBar.dismiss()});dialogActionSubscription=St({source:this.dialogRef,computation:(t,e)=>{}});snackBarActionSubscription=St({source:this.snackBarRef,computation:(t,e)=>(e&&e.value?.unsubscribe(),t?.onAction()?.subscribe(()=>this.bossfightDataService.reload()))});selectedBossfight=St({source:this.bossfightData,computation:(t,e)=>{let{selectedDocument:r}=this.userData();if(!t?.length)return r;let i=new Set(t?.map(({id:s})=>s)),o=e?.value??r;return i.has(o)?o:t?.at(-1)?.id??""}});onSelectionChange=bt(()=>{let t=this.selectedBossfight(),e=this.userData()??{selectedDocument:""};t&&e?.selectedDocument!==t&&(console.log("Updating user data with selected bossfight:",t),this.userDataService.saveUserData(W(w({},e),{selectedDocument:t})))});selectedDocumentRecord=Me(()=>{if(!this.bossfightDataLoading()&&this.bossfightData()){let t=this.bossfightData()?.find(({id:e})=>e==this.selectedBossfight());return t?this.mergeVariantData(t):void 0}},{equal:Ld});updateAppBackground=bt(()=>{let t=this.selectedDocumentRecord()?.baseColor??null;this.appColorService.updateColor(t)});mergeVariantData(t){let{displayVariant:e}=this.userData().documentStates[t.id]??{};return e&&t.variant?W(w({},t.variant),{id:t.id}):t}listenForTransitionSignal(t){let{key:e,ctrlKey:r,shiftKey:i}=t;if(r&&i&&e==="L"){let o=structuredClone(this.userData()),s=o.documentStates[this.selectedBossfight()]??{},{displayVariant:a=!1,transitionPlayed:c=!1}=s,l=structuredClone(s),d=this.selectedDocumentRecord();if(l.displayVariant=!a,a)o.documentStates[this.selectedBossfight()]=l,this.userDataService.saveUserData(o);else if(l.displayVariant&&!c&&d?.variantTransition){if(this.dialogRef())return;let u=this._dialog.open(this.transitionPlayerTemplate(),{data:d.variantTransition,height:"100dvh",width:"100dvw",minHeight:"100dvh",minWidth:"100dvw",maxWidth:"100dvw",maxHeight:"100dvh",closePredicate:m=>m==="transitionEnd"});u.afterClosed().subscribe(m=>{l.transitionPlayed=!0,o.documentStates[this.selectedBossfight()]=l,this.userDataService.saveUserData(o)}),this.dialogRef.set(u)}else o.documentStates[this.selectedBossfight()]=l,this.userDataService.saveUserData(o)}}transitionEnded(){this.dialogRef()?.close("transitionEnd")}static \u0275fac=function(e){return new(e||n)};static \u0275cmp=q({type:n,selectors:[["app-root"]],viewQuery:function(e,r){e&1&&So(r.transitionPlayerTemplate,CO,5),e&2&&To()},hostBindings:function(e,r){e&1&&Ce("keydown",function(o){return r.listenForTransitionSignal(o)},Nm)},decls:6,vars:1,consts:[["bossfightOptionContent",""],["transitionPlayer",""],[1,"app-loading"],["mode","indeterminate"],[1,"mat-primary-container"],["appearance","outline"],[3,"ngModelChange","ngModel","value"],[1,"bossfight-data-render"],[1,"bossfight-data-render",3,"value"],[1,"spacer"],[1,"content-container"],[3,"documentRecord"],[4,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"bossfight-data-wrapper"],["alt","",3,"src"],["autoplay","",2,"height","100dvh","width","100dvw",3,"ended","src"]],template:function(e,r){e&1&&(pe(0,IO,4,0,"div",2)(1,OO,11,6),Ue(2,NO,3,2,"ng-template",null,0,Pr)(4,PO,1,1,"ng-template",null,1,Pr)),e&2&&ge(r.bossfightDataLoading()?0:1)},dependencies:[Cx,rD,Id,Qx,$x,Cp,tp,aD,iD,Ai,Bd],styles:["[_nghost-%COMP%]{display:grid;grid-row:var(--mat-toolbar-standard-height) auto}mat-toolbar[_ngcontent-%COMP%]{position:fixed;padding-block:32px;padding-inline:16px;z-index:10}.content-container[_ngcontent-%COMP%]{padding-top:calc(var(--mat-toolbar-standard-height) + 16px)}mat-toolbar[_ngcontent-%COMP%]   mat-form-field[_ngcontent-%COMP%]{flex:1 0 auto}.app-loading[_ngcontent-%COMP%]{display:grid;width:100dvw;height:100dvh;align-content:center;justify-content:center}mat-select.bossfight-select[_ngcontent-%COMP%]{max-width:fit-content;min-width:56px}.bossfight-data-render[_ngcontent-%COMP%]   .bossfight-data-wrapper[_ngcontent-%COMP%]{display:inline-flex;flex-wrap:nowrap;align-items:center;height:32px;gap:8px}.bossfight-data-render[_ngcontent-%COMP%]   .bossfight-data-wrapper[_ngcontent-%COMP%]   img[_ngcontent-%COMP%]{height:32px}.bossfight-data-render[_ngcontent-%COMP%]   .bossfight-data-wrapper[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{overflow:hidden;text-overflow:ellipsis}"]})};Ah(Zd,wx).catch(n=>console.error(n));
