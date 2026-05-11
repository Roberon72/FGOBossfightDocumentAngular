var Yx=Object.defineProperty,Qx=Object.defineProperties;var Kx=Object.getOwnPropertyDescriptors;var Ds=Object.getOwnPropertySymbols;var Eh=Object.prototype.hasOwnProperty,kh=Object.prototype.propertyIsEnumerable;var Ch=(t,n,e)=>n in t?Yx(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,y=(t,n)=>{for(var e in n||={})Eh.call(n,e)&&Ch(t,e,n[e]);if(Ds)for(var e of Ds(n))kh.call(n,e)&&Ch(t,e,n[e]);return t},B=(t,n)=>Qx(t,Kx(n));var Ih=(t,n)=>{var e={};for(var i in t)Eh.call(t,i)&&n.indexOf(i)<0&&(e[i]=t[i]);if(t!=null&&Ds)for(var i of Ds(t))n.indexOf(i)<0&&kh.call(t,i)&&(e[i]=t[i]);return e};var We=null,Cs=!1,Sl=1,Xx=null,Ce=Symbol("SIGNAL");function I(t){let n=We;return We=t,n}function Es(){return We}var Rn={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function On(t){if(Cs)throw new Error("");if(We===null)return;We.consumerOnSignalRead(t);let n=We.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=We.recomputing;if(i&&(e=n!==void 0?n.nextProducer:We.producers,e!==void 0&&e.producer===t)){We.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===We&&(!i||e0(r,We)))return;let o=sr(We),s={producer:t,consumer:We,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};We.producersTail=s,n!==void 0?n.nextProducer=s:We.producers=s,o&&Ah(t,s)}function Mh(){Sl++}function si(t){if(!(sr(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===Sl)){if(!t.producerMustRecompute(t)&&!or(t)){rr(t);return}t.producerRecomputeValue(t),rr(t)}}function Tl(t){if(t.consumers===void 0)return;let n=Cs;Cs=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||Jx(i)}}finally{Cs=n}}function Al(){return We?.consumerAllowSignalWrites!==!1}function Jx(t){t.dirty=!0,Tl(t),t.consumerMarkedDirty?.(t)}function rr(t){t.dirty=!1,t.lastCleanEpoch=Sl}function mn(t){return t&&Sh(t),I(t)}function Sh(t){t.producersTail=void 0,t.recomputing=!0}function Nn(t,n){I(n),t&&Th(t)}function Th(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(sr(t))do e=Rl(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function or(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(si(e),i!==e.version))return!0}return!1}function Fn(t){if(sr(t)){let n=t.producers;for(;n!==void 0;)n=Rl(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function Ah(t,n){let e=t.consumersTail,i=sr(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)Ah(r.producer,r)}function Rl(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!sr(n)){let o=n.producers;for(;o!==void 0;)o=Rl(o)}return e}function sr(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Qr(t){Xx?.(t)}function e0(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function Kr(t,n){return Object.is(t,n)}function Xr(t,n){let e=Object.create(t0);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(si(e),On(e),e.value===Qt)throw e.error;return e.value};return i[Ce]=e,Qr(e),i}var ri=Symbol("UNSET"),oi=Symbol("COMPUTING"),Qt=Symbol("ERRORED"),t0=B(y({},Rn),{value:ri,dirty:!0,error:null,equal:Kr,kind:"computed",producerMustRecompute(t){return t.value===ri||t.value===oi},producerRecomputeValue(t){if(t.value===oi)throw new Error("");let n=t.value;t.value=oi;let e=mn(t),i,r=!1;try{i=t.computation(),I(null),r=n!==ri&&n!==Qt&&i!==Qt&&t.equal(n,i)}catch(o){i=Qt,t.error=o}finally{Nn(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function n0(){throw new Error}var Rh=n0;function Oh(t){Rh(t)}function Ol(t){Rh=t}var i0=null;function Nl(t,n){let e=Object.create(Jr);e.value=t,n!==void 0&&(e.equal=n);let i=()=>Nh(e);return i[Ce]=e,Qr(e),[i,s=>ai(e,s),s=>ks(e,s)]}function Nh(t){return On(t),t.value}function ai(t,n){Al()||Oh(t),t.equal(t.value,n)||(t.value=n,r0(t))}function ks(t,n){Al()||Oh(t),ai(t,n(t.value))}var Jr=B(y({},Rn),{equal:Kr,value:void 0,kind:"signal"});function r0(t){t.version++,Mh(),Tl(t),i0?.(t)}var Fl=B(y({},Rn),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Pl(t){if(t.dirty=!1,t.version>0&&!or(t))return;t.version++;let n=mn(t);try{t.cleanup(),t.fn()}finally{Nn(t,n)}}function Y(t){return typeof t=="function"}function Is(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Ms=Is(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function ci(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var ie=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(Y(i))try{i()}catch(o){n=o instanceof Ms?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{Fh(o)}catch(s){n=n??[],s instanceof Ms?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Ms(n)}}add(n){var e;if(n&&n!==this)if(this.closed)Fh(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&ci(e,n)}remove(n){let{_finalizers:e}=this;e&&ci(e,n),n instanceof t&&n._removeParent(this)}};ie.EMPTY=(()=>{let t=new ie;return t.closed=!0,t})();var Ll=ie.EMPTY;function Ss(t){return t instanceof ie||t&&"closed"in t&&Y(t.remove)&&Y(t.add)&&Y(t.unsubscribe)}function Fh(t){Y(t)?t():t.unsubscribe()}var St={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ar={setTimeout(t,n,...e){let{delegate:i}=ar;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=ar;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Ts(t){ar.setTimeout(()=>{let{onUnhandledError:n}=St;if(n)n(t);else throw t})}function eo(){}var Ph=Vl("C",void 0,void 0);function Lh(t){return Vl("E",void 0,t)}function Vh(t){return Vl("N",t,void 0)}function Vl(t,n,e){return{kind:t,value:n,error:e}}var li=null;function cr(t){if(St.useDeprecatedSynchronousErrorHandling){let n=!li;if(n&&(li={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=li;if(li=null,e)throw i}}else t()}function Bh(t){St.useDeprecatedSynchronousErrorHandling&&li&&(li.errorThrown=!0,li.error=t)}var di=class extends ie{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Ss(n)&&n.add(this)):this.destination=a0}static create(n,e,i){return new hn(n,e,i)}next(n){this.isStopped?jl(Vh(n),this):this._next(n)}error(n){this.isStopped?jl(Lh(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?jl(Ph,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},o0=Function.prototype.bind;function Bl(t,n){return o0.call(t,n)}var Hl=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){As(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){As(i)}else As(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){As(e)}}},hn=class extends di{constructor(n,e,i){super();let r;if(Y(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&St.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Bl(n.next,o),error:n.error&&Bl(n.error,o),complete:n.complete&&Bl(n.complete,o)}):r=n}this.destination=new Hl(r)}};function As(t){St.useDeprecatedSynchronousErrorHandling?Bh(t):Ts(t)}function s0(t){throw t}function jl(t,n){let{onStoppedNotification:e}=St;e&&ar.setTimeout(()=>e(t,n))}var a0={closed:!0,next:eo,error:s0,complete:eo};var lr=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Tt(t){return t}function jh(t){return t.length===0?Tt:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var K=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=l0(e)?e:new hn(e,i,r);return cr(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Hh(i),new i((r,o)=>{let s=new hn({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[lr](){return this}pipe(...e){return jh(e)(this)}toPromise(e){return e=Hh(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return t.create=n=>new t(n),t})();function Hh(t){var n;return(n=t??St.Promise)!==null&&n!==void 0?n:Promise}function c0(t){return t&&Y(t.next)&&Y(t.error)&&Y(t.complete)}function l0(t){return t&&t instanceof di||c0(t)&&Ss(t)}function d0(t){return Y(t?.lift)}function J(t){return n=>{if(d0(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function re(t,n,e,i,r){return new zl(t,n,e,i,r)}var zl=class extends di{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var zh=Is(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var D=(()=>{class t extends K{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Rs(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new zh}next(e){cr(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){cr(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){cr(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?Ll:(this.currentObservers=null,o.push(e),new ie(()=>{this.currentObservers=null,ci(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new K;return e.source=this,e}}return t.create=(n,e)=>new Rs(n,e),t})(),Rs=class extends D{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:Ll}};var ui=class extends D{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var to={now(){return(to.delegate||Date).now()},delegate:void 0};var Os=class extends D{constructor(n=1/0,e=1/0,i=to){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var Ns=class extends ie{constructor(n,e){super()}schedule(n,e=0){return this}};var no={setInterval(t,n,...e){let{delegate:i}=no;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=no;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var Fs=class extends Ns{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return no.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&no.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,ci(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var dr=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};dr.now=to.now;var Ps=class extends dr{constructor(n,e=dr.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var io=new Ps(Fs),Uh=io;var fi=new K(t=>t.complete());function Ls(t){return t&&Y(t.schedule)}function Ul(t){return t[t.length-1]}function Vs(t){return Y(Ul(t))?t.pop():void 0}function Kt(t){return Ls(Ul(t))?t.pop():void 0}function $h(t,n){return typeof Ul(t)=="number"?t.pop():n}function Gh(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(u){s(u)}}function c(d){try{l(i.throw(d))}catch(u){s(u)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(t,n||[])).next())})}function Wh(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function mi(t){return this instanceof mi?(this.v=t,this):new mi(t)}function qh(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(h){return function(p){return Promise.resolve(p).then(h,u)}}function a(h,p){i[h]&&(r[h]=function(b){return new Promise(function(_,E){o.push([h,b,_,E])>1||c(h,b)})},p&&(r[h]=p(r[h])))}function c(h,p){try{l(i[h](p))}catch(b){m(o[0][3],b)}}function l(h){h.value instanceof mi?Promise.resolve(h.value.v).then(d,u):m(o[0][2],h)}function d(h){c("next",h)}function u(h){c("throw",h)}function m(h,p){h(p),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Zh(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof Wh=="function"?Wh(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,c){s=t[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var Bs=t=>t&&typeof t.length=="number"&&typeof t!="function";function js(t){return Y(t?.then)}function Hs(t){return Y(t[lr])}function zs(t){return Symbol.asyncIterator&&Y(t?.[Symbol.asyncIterator])}function Us(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function u0(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var $s=u0();function Ws(t){return Y(t?.[$s])}function Gs(t){return qh(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield mi(e.read());if(r)return yield mi(void 0);yield yield mi(i)}}finally{e.releaseLock()}})}function qs(t){return Y(t?.getReader)}function me(t){if(t instanceof K)return t;if(t!=null){if(Hs(t))return f0(t);if(Bs(t))return m0(t);if(js(t))return h0(t);if(zs(t))return Yh(t);if(Ws(t))return p0(t);if(qs(t))return g0(t)}throw Us(t)}function f0(t){return new K(n=>{let e=t[lr]();if(Y(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function m0(t){return new K(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function h0(t){return new K(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Ts)})}function p0(t){return new K(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Yh(t){return new K(n=>{b0(t,n).catch(e=>n.error(e))})}function g0(t){return Yh(Gs(t))}function b0(t,n){var e,i,r,o;return Gh(this,void 0,void 0,function*(){try{for(e=Zh(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function et(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Zs(t,n=0){return J((e,i)=>{e.subscribe(re(i,r=>et(i,t,()=>i.next(r),n),()=>et(i,t,()=>i.complete(),n),r=>et(i,t,()=>i.error(r),n)))})}function Ys(t,n=0){return J((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function Qh(t,n){return me(t).pipe(Ys(n),Zs(n))}function Kh(t,n){return me(t).pipe(Ys(n),Zs(n))}function Xh(t,n){return new K(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function Jh(t,n){return new K(e=>{let i;return et(e,n,()=>{i=t[$s](),et(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>Y(i?.return)&&i.return()})}function Qs(t,n){if(!t)throw new Error("Iterable cannot be null");return new K(e=>{et(e,n,()=>{let i=t[Symbol.asyncIterator]();et(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function ep(t,n){return Qs(Gs(t),n)}function tp(t,n){if(t!=null){if(Hs(t))return Qh(t,n);if(Bs(t))return Xh(t,n);if(js(t))return Kh(t,n);if(zs(t))return Qs(t,n);if(Ws(t))return Jh(t,n);if(qs(t))return ep(t,n)}throw Us(t)}function mt(t,n){return n?tp(t,n):me(t)}function Xt(...t){let n=Kt(t);return mt(t,n)}function np(t){return t instanceof Date&&!isNaN(t)}function ge(t,n){return J((e,i)=>{let r=0;e.subscribe(re(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:_0}=Array;function v0(t,n){return _0(n)?t(...n):t(n)}function Ks(t){return ge(n=>v0(t,n))}var{isArray:y0}=Array,{getPrototypeOf:x0,prototype:w0,keys:D0}=Object;function Xs(t){if(t.length===1){let n=t[0];if(y0(n))return{args:n,keys:null};if(C0(n)){let e=D0(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function C0(t){return t&&typeof t=="object"&&x0(t)===w0}function Js(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function $l(...t){let n=Kt(t),e=Vs(t),{args:i,keys:r}=Xs(t);if(i.length===0)return mt([],n);let o=new K(E0(i,n,r?s=>Js(r,s):Tt));return e?o.pipe(Ks(e)):o}function E0(t,n,e=Tt){return i=>{ip(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)ip(n,()=>{let l=mt(t[c],n),d=!1;l.subscribe(re(i,u=>{o[c]=u,d||(d=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function ip(t,n,e){t?et(e,t,n):n()}function rp(t,n,e,i,r,o,s,a){let c=[],l=0,d=0,u=!1,m=()=>{u&&!c.length&&!l&&n.complete()},h=b=>l<i?p(b):c.push(b),p=b=>{o&&n.next(b),l++;let _=!1;me(e(b,d++)).subscribe(re(n,E=>{r?.(E),o?h(E):n.next(E)},()=>{_=!0},void 0,()=>{if(_)try{for(l--;c.length&&l<i;){let E=c.shift();s?et(n,s,()=>p(E)):p(E)}m()}catch(E){n.error(E)}}))};return t.subscribe(re(n,h,()=>{u=!0,m()})),()=>{a?.()}}function ur(t,n,e=1/0){return Y(n)?ur((i,r)=>ge((o,s)=>n(i,o,r,s))(me(t(i,r))),e):(typeof n=="number"&&(e=n),J((i,r)=>rp(i,r,t,e)))}function ea(t=1/0){return ur(Tt,t)}function op(){return ea(1)}function fr(...t){return op()(mt(t,Kt(t)))}function Wl(t){return new K(n=>{me(t()).subscribe(n)})}function Gl(...t){let n=Vs(t),{args:e,keys:i}=Xs(t),r=new K(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let u=!1;me(e[d]).subscribe(re(o,m=>{u||(u=!0,l--),a[d]=m},()=>c--,void 0,()=>{(!c||!u)&&(l||o.next(i?Js(i,a):a),o.complete())}))}});return n?r.pipe(Ks(n)):r}function sp(t=0,n,e=Uh){let i=-1;return n!=null&&(Ls(n)?e=n:i=n),new K(r=>{let o=np(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function pn(...t){let n=Kt(t),e=$h(t,1/0),i=t;return i.length?i.length===1?me(i[0]):ea(e)(mt(i,n)):fi}function ye(t,n){return J((e,i)=>{let r=0;e.subscribe(re(i,o=>t.call(n,o,r++)&&i.next(o)))})}function ap(t){return J((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,e.next(l)}s&&e.complete()},c=()=>{o=null,s&&e.complete()};n.subscribe(re(e,l=>{i=!0,r=l,o||me(t(l)).subscribe(o=re(e,a,c))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function ta(t,n=io){return ap(()=>sp(t,n))}function ql(t,n){return Y(n)?ur(t,n,1):ur(t,1)}function ro(t,n=io){return J((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+t,d=n.now();if(d<l){r=this.schedule(void 0,l-d),i.add(r);return}a()}e.subscribe(re(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function hi(t){return t<=0?()=>fi:J((n,e)=>{let i=0;n.subscribe(re(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function na(t,n=Tt){return t=t??k0,J((e,i)=>{let r,o=!0;e.subscribe(re(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function k0(t,n){return t===n}function Zl(t){return J((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function ia(){return J((t,n)=>{let e,i=!1;t.subscribe(re(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function cp(t={}){let{connector:n=()=>new D,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,c,l=0,d=!1,u=!1,m=()=>{a?.unsubscribe(),a=void 0},h=()=>{m(),s=c=void 0,d=u=!1},p=()=>{let b=s;h(),b?.unsubscribe()};return J((b,_)=>{l++,!u&&!d&&m();let E=c=c??n();_.add(()=>{l--,l===0&&!u&&!d&&(a=Yl(p,r))}),E.subscribe(_),!s&&l>0&&(s=new hn({next:le=>E.next(le),error:le=>{u=!0,m(),a=Yl(h,e,le),E.error(le)},complete:()=>{d=!0,m(),a=Yl(h,i),E.complete()}}),me(b).subscribe(s))})(o)}}function Yl(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new hn({next:()=>{i.unsubscribe(),t()}});return me(n(...e)).subscribe(i)}function ra(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,cp({connector:()=>new Os(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function oo(t){return ye((n,e)=>t<=e)}function ht(...t){let n=Kt(t);return J((e,i)=>{(n?fr(t,e,n):fr(t,e)).subscribe(i)})}function pi(t,n){return J((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(re(i,c=>{r?.unsubscribe();let l=0,d=o++;me(t(c,d)).subscribe(r=re(i,u=>i.next(n?n(c,u,d,l++):u),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function Pe(t){return J((n,e)=>{me(t).subscribe(re(e,()=>e.complete(),eo)),!e.closed&&n.subscribe(e)})}function Ql(t,n=!1){return J((e,i)=>{let r=0;e.subscribe(re(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function Kl(t,n,e){let i=Y(t)||n||e?{next:t,error:n,complete:e}:t;return i?J((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(re(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):Tt}var Xl;function oa(){return Xl}function Jt(t){let n=Xl;return Xl=t,n}var lp=Symbol("NotFound");function mr(t){return t===lp||t?.name==="\u0275NotFound"}function Jl(t,n,e){let i=Object.create(I0);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(si(i),On(i),i.value===Qt)throw i.error;return i.value};return o[Ce]=i,Qr(i),o}function dp(t,n){si(t),ai(t,n),rr(t)}function up(t,n){if(si(t),t.value===Qt)throw t.error;ks(t,n),rr(t)}var I0=B(y({},Rn),{value:ri,dirty:!0,error:null,equal:Kr,kind:"linkedSignal",producerMustRecompute(t){return t.value===ri||t.value===oi},producerRecomputeValue(t){if(t.value===oi)throw new Error("");let n=t.value;t.value=oi;let e=mn(t),i,r=!1;try{let o=t.source(),s=n!==ri&&n!==Qt,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,I(null),r=s&&i!==Qt&&t.equal(n,i)}catch(o){i=Qt,t.error=o}finally{Nn(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function fp(t){let n=I(null);try{return t()}finally{I(n)}}var fa="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",k=class extends Error{code;constructor(n,e){super(Vn(n,e)),this.code=n}};function M0(t){return`NG0${Math.abs(t)}`}function Vn(t,n){return`${M0(t)}${n?": "+n:""}`}var Bn=globalThis;function de(t){for(let n in t)if(t[n]===de)return n;throw Error("")}function bp(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function ma(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(ma).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function ha(t,n){return t?n?`${t} ${n}`:t:n||""}var S0=de({__forward_ref__:de});function pt(t){return t.__forward_ref__=pt,t}function Ue(t){return fd(t)?t():t}function fd(t){return typeof t=="function"&&t.hasOwnProperty(S0)&&t.__forward_ref__===pt}function v(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function xe(t){return{providers:t.providers||[],imports:t.imports||[]}}function pa(t){return T0(t,ga)}function T0(t,n){return t.hasOwnProperty(n)&&t[n]||null}function A0(t){let n=t?.[ga]??null;return n||null}function td(t){return t&&t.hasOwnProperty(aa)?t[aa]:null}var ga=de({\u0275prov:de}),aa=de({\u0275inj:de}),g=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=v({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function md(t){return t&&!!t.\u0275providers}var hd=de({\u0275cmp:de}),pd=de({\u0275dir:de}),gd=de({\u0275pipe:de});var ao=de({\u0275fac:de}),xi=de({__NG_ELEMENT_ID__:de}),mp=de({__NG_ENV_ID__:de});function jn(t){return _d(t,"@Component"),t[hd]||null}function bd(t){return _d(t,"@Directive"),t[pd]||null}function _p(t){return _d(t,"@Pipe"),t[gd]||null}function _d(t,n){if(t==null)throw new k(-919,!1)}function mo(t){return typeof t=="string"?t:t==null?"":String(t)}var vp=de({ngErrorCode:de}),R0=de({ngErrorMessage:de}),O0=de({ngTokenPath:de});function vd(t,n){return yp("",-200,n)}function ba(t,n){throw new k(-201,!1)}function yp(t,n,e){let i=new k(n,t);return i[vp]=n,i[R0]=t,e&&(i[O0]=e),i}function N0(t){return t[vp]}var nd;function xp(){return nd}function rt(t){let n=nd;return nd=t,n}function yd(t,n,e){let i=pa(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;ba(t,"")}var F0={},gi=F0,P0="__NG_DI_FLAG__",id=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=bi(e)||0;try{return this.injector.get(n,i&8?null:gi,i)}catch(r){if(mr(r))return r;throw r}}};function L0(t,n=0){let e=oa();if(e===void 0)throw new k(-203,!1);if(e===null)return yd(t,void 0,n);{let i=V0(n),r=e.retrieve(t,i);if(mr(r)){if(i.optional)return null;throw r}return r}}function R(t,n=0){return(xp()||L0)(Ue(t),n)}function f(t,n){return R(t,bi(n))}function bi(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function V0(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function rd(t){let n=[];for(let e=0;e<t.length;e++){let i=Ue(t[e]);if(Array.isArray(i)){if(i.length===0)throw new k(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=B0(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(R(r,o))}else n.push(R(i))}return n}function B0(t){return t[P0]}function _i(t,n){let e=t.hasOwnProperty(ao);return e?t[ao]:null}function wp(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function Dp(t){return t.flat(Number.POSITIVE_INFINITY)}function _a(t,n){t.forEach(e=>Array.isArray(e)?_a(e,n):n(e))}function xd(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function ho(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function Cp(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function Ep(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function va(t,n,e){let i=pr(t,n);return i>=0?t[i|1]=e:(i=~i,Ep(t,i,n,e)),i}function ya(t,n){let e=pr(t,n);if(e>=0)return t[e|1]}function pr(t,n){return j0(t,n,1)}function j0(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Hn={},Ge=[],wi=new g(""),wd=new g("",-1),Dd=new g(""),co=class{get(n,e=gi){if(e===gi){let r=yp("",-201);throw r.name="\u0275NotFound",r}return e}};function gr(t){return{\u0275providers:t}}function kp(t){return gr([{provide:wi,multi:!0,useValue:t}])}function Ip(...t){return{\u0275providers:Cd(!0,t),\u0275fromNgModule:!0}}function Cd(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return _a(n,s=>{let a=s;ca(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&Mp(r,o),e}function Mp(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Ed(r,o=>{n(o,i)})}}function ca(t,n,e,i){if(t=Ue(t),!t)return!1;let r=null,o=td(t),s=!o&&jn(t);if(!o&&!s){let c=t.ngModule;if(o=td(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)ca(l,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;_a(o.imports,d=>{ca(d,n,e,i)&&(l||=[],l.push(d))}),l!==void 0&&Mp(l,n)}if(!a){let l=_i(r)||(()=>new r);n({provide:r,useFactory:l,deps:Ge},r),n({provide:Dd,useValue:r,multi:!0},r),n({provide:wi,useValue:()=>R(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=t;Ed(c,d=>{n(d,l)})}}else return!1;return r!==t&&t.providers!==void 0}function Ed(t,n){for(let e of t)md(e)&&(e=e.\u0275providers),Array.isArray(e)?Ed(e,n):n(e)}var H0=de({provide:String,useValue:de});function Sp(t){return t!==null&&typeof t=="object"&&H0 in t}function z0(t){return!!(t&&t.useExisting)}function U0(t){return!!(t&&t.useFactory)}function vi(t){return typeof t=="function"}function Tp(t){return!!t.useClass}var po=new g(""),sa={},hp={},ed;function br(){return ed===void 0&&(ed=new co),ed}var Ee=class{},yi=class extends Ee{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,sd(n,s=>this.processProvider(s)),this.records.set(wd,hr(void 0,this)),r.has("environment")&&this.records.set(Ee,hr(void 0,this));let o=this.records.get(po);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Dd,Ge,{self:!0}))}retrieve(n,e){let i=bi(e)||0;try{return this.get(n,gi,i)}catch(r){if(mr(r))return r;throw r}}destroy(){so(this),this._destroyed=!0;let n=I(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),I(n)}}onDestroy(n){return so(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){so(this);let e=Jt(this),i=rt(void 0),r;try{return n()}finally{Jt(e),rt(i)}}get(n,e=gi,i){if(so(this),n.hasOwnProperty(mp))return n[mp](this);let r=bi(i),o,s=Jt(this),a=rt(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let d=Z0(n)&&pa(n);d&&this.injectableDefInScope(d)?l=hr(od(n),sa):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?br():this.parent;return e=r&8&&e===gi?null:e,c.get(n,e)}catch(c){let l=N0(c);throw l===-200||l===-201?new k(l,null):c}finally{rt(a),Jt(s)}}resolveInjectorInitializers(){let n=I(null),e=Jt(this),i=rt(void 0),r;try{let o=this.get(wi,Ge,{self:!0});for(let s of o)s()}finally{Jt(e),rt(i),I(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Ue(n);let e=vi(n)?n:Ue(n&&n.provide),i=W0(n);if(!vi(n)&&n.multi===!0){let r=this.records.get(e);r||(r=hr(void 0,sa,!0),r.factory=()=>rd(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=I(null);try{if(e.value===hp)throw vd("");return e.value===sa&&(e.value=hp,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&q0(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{I(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=Ue(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function od(t){let n=pa(t),e=n!==null?n.factory:_i(t);if(e!==null)return e;if(t instanceof g)throw new k(-204,!1);if(t instanceof Function)return $0(t);throw new k(-204,!1)}function $0(t){if(t.length>0)throw new k(-204,!1);let e=A0(t);return e!==null?()=>e.factory(t):()=>new t}function W0(t){if(Sp(t))return hr(void 0,t.useValue);{let n=kd(t);return hr(n,sa)}}function kd(t,n,e){let i;if(vi(t)){let r=Ue(t);return _i(r)||od(r)}else if(Sp(t))i=()=>Ue(t.useValue);else if(U0(t))i=()=>t.useFactory(...rd(t.deps||[]));else if(z0(t))i=(r,o)=>R(Ue(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Ue(t&&(t.useClass||t.provide));if(G0(t))i=()=>new r(...rd(t.deps));else return _i(r)||od(r)}return i}function so(t){if(t.destroyed)throw new k(-205,!1)}function hr(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function G0(t){return!!t.deps}function q0(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function Z0(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function sd(t,n){for(let e of t)Array.isArray(e)?sd(e,n):e&&md(e)?sd(e.\u0275providers,n):n(e)}function _r(t,n){let e;t instanceof yi?(so(t),e=t):e=new id(t);let i,r=Jt(e),o=rt(void 0);try{return n()}finally{Jt(r),rt(o)}}function Ap(){return xp()!==void 0||oa()!=null}var Rt=0,M=1,A=2,Oe=3,gt=4,Xe=5,Di=6,vr=7,ke=8,bn=9,Ot=10,ue=11,yr=12,Id=13,Ci=14,Je=15,zn=16,Ei=17,tn=18,_n=19,Md=20,gn=21,xa=22,Pn=23,ot=24,ki=25,Un=26,be=27,Rp=1,Sd=6,$n=7,go=8,Ii=9,we=10;function vn(t){return Array.isArray(t)&&typeof t[Rp]=="object"}function Nt(t){return Array.isArray(t)&&t[Rp]===!0}function Td(t){return(t.flags&4)!==0}function yn(t){return t.componentOffset>-1}function xr(t){return(t.flags&1)===1}function nn(t){return!!t.template}function wr(t){return(t[A]&512)!==0}function Mi(t){return(t[A]&256)===256}var Ad="svg",Op="math";function bt(t){for(;Array.isArray(t);)t=t[Rt];return t}function Rd(t,n){return bt(n[t])}function Ft(t,n){return bt(n[t.index])}function wa(t,n){return t.data[n]}function Od(t,n){return t[n]}function Da(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function _t(t,n){let e=n[t];return vn(e)?e:e[Rt]}function Np(t){return(t[A]&4)===4}function Ca(t){return(t[A]&128)===128}function Fp(t){return Nt(t[Oe])}function vt(t,n){return n==null?null:t[n]}function Nd(t){t[Ei]=0}function Fd(t){t[A]&1024||(t[A]|=1024,Ca(t)&&Si(t))}function Pp(t,n){for(;t>0;)n=n[Ci],t--;return n}function bo(t){return!!(t[A]&9216||t[ot]?.dirty)}function Ea(t){t[Ot].changeDetectionScheduler?.notify(8),t[A]&64&&(t[A]|=1024),bo(t)&&Si(t)}function Si(t){t[Ot].changeDetectionScheduler?.notify(0);let n=Ln(t);for(;n!==null&&!(n[A]&8192||(n[A]|=8192,!Ca(n)));)n=Ln(n)}function Pd(t,n){if(Mi(t))throw new k(911,!1);t[gn]===null&&(t[gn]=[]),t[gn].push(n)}function Lp(t,n){if(t[gn]===null)return;let e=t[gn].indexOf(n);e!==-1&&t[gn].splice(e,1)}function Ln(t){let n=t[Oe];return Nt(n)?n[Oe]:n}function Ld(t){return t[vr]??=[]}function Vd(t){return t.cleanup??=[]}function Vp(t,n,e,i){let r=Ld(n);r.push(e),t.firstCreatePass&&Vd(t).push(i,r.length-1)}var z={lFrame:Zp(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var ad=!1;function Bp(){return z.lFrame.elementDepthCount}function jp(){z.lFrame.elementDepthCount++}function Bd(){z.lFrame.elementDepthCount--}function ka(){return z.bindingsEnabled}function jd(){return z.skipHydrationRootTNode!==null}function Hd(t){return z.skipHydrationRootTNode===t}function zd(){z.skipHydrationRootTNode=null}function T(){return z.lFrame.lView}function he(){return z.lFrame.tView}function Pt(t){return z.lFrame.contextLView=t,t[ke]}function Lt(t){return z.lFrame.contextLView=null,t}function Le(){let t=Ud();for(;t!==null&&t.type===64;)t=t.parent;return t}function Ud(){return z.lFrame.currentTNode}function Hp(){let t=z.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Ti(t,n){let e=z.lFrame;e.currentTNode=t,e.isParent=n}function $d(){return z.lFrame.isParent}function Wd(){z.lFrame.isParent=!1}function Gd(){return z.lFrame.contextLView}function qd(){return ad}function lo(t){let n=ad;return ad=t,n}function Zd(){let t=z.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function zp(t){return z.lFrame.bindingIndex=t}function Wn(){return z.lFrame.bindingIndex++}function Yd(t){let n=z.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function Up(){return z.lFrame.inI18n}function $p(t,n){let e=z.lFrame;e.bindingIndex=e.bindingRootIndex=t,Ia(n)}function Wp(){return z.lFrame.currentDirectiveIndex}function Ia(t){z.lFrame.currentDirectiveIndex=t}function Gp(t){let n=z.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Ma(){return z.lFrame.currentQueryIndex}function _o(t){z.lFrame.currentQueryIndex=t}function Y0(t){let n=t[M];return n.type===2?n.declTNode:n.type===1?t[Xe]:null}function Qd(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=Y0(o),r===null||(o=o[Ci],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=z.lFrame=qp();return i.currentTNode=n,i.lView=t,!0}function Sa(t){let n=qp(),e=t[M];z.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function qp(){let t=z.lFrame,n=t===null?null:t.child;return n===null?Zp(t):n}function Zp(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function Yp(){let t=z.lFrame;return z.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Kd=Yp;function Ta(){let t=Yp();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Qp(t){return(z.lFrame.contextLView=Pp(t,z.lFrame.contextLView))[ke]}function rn(){return z.lFrame.selectedIndex}function Gn(t){z.lFrame.selectedIndex=t}function vo(){let t=z.lFrame;return wa(t.tView,t.selectedIndex)}function Vt(){z.lFrame.currentNamespace=Ad}function Dr(){Q0()}function Q0(){z.lFrame.currentNamespace=null}function Kp(){return z.lFrame.currentNamespace}var Xp=!0;function Aa(){return Xp}function yo(t){Xp=t}function cd(t,n=null,e=null,i){let r=Jp(t,n,e,i);return r.resolveInjectorInitializers(),r}function Jp(t,n=null,e=null,i,r=new Set){let o=[e||Ge,Ip(t)],s;return new yi(o,n||br(),s||null,r)}var Z=class t{static THROW_IF_NOT_FOUND=gi;static NULL=new co;static create(n,e){if(Array.isArray(n))return cd({name:""},e,n,"");{let i=n.name??"";return cd({name:i},n.parent,n.providers,i)}}static \u0275prov=v({token:t,providedIn:"any",factory:()=>R(wd)});static __NG_ELEMENT_ID__=-1},P=new g(""),st=(()=>{class t{static __NG_ELEMENT_ID__=K0;static __NG_ENV_ID__=e=>e}return t})(),la=class extends st{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Mi(this._lView)}onDestroy(n){let e=this._lView;return Pd(e,n),()=>Lp(e,n)}};function K0(){return new la(T())}var eg=!1,tg=new g(""),Ai=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new ui(!1);debugTaskTracker=f(tg,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new K(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=v({token:t,providedIn:"root",factory:()=>new t})}return t})(),ld=class extends D{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Ap()&&(this.destroyRef=f(st,{optional:!0})??void 0,this.pendingTasks=f(Ai,{optional:!0})??void 0)}emit(n){let e=I(null);try{super.next(n)}finally{I(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof ie&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},F=ld;function da(...t){}function Xd(t){let n,e;function i(){t=da;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function ng(t){return queueMicrotask(()=>t()),()=>{t=da}}var Jd="isAngularZone",uo=Jd+"_ID",X0=0,S=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new F(!1);onMicrotaskEmpty=new F(!1);onStable=new F(!1);onError=new F(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=eg}=n;if(typeof Zone>"u")throw new k(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,tw(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Jd)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new k(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new k(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,J0,da,da);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},J0={};function eu(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function ew(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Xd(()=>{t.callbackScheduled=!1,dd(t),t.isCheckStableRunning=!0,eu(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),dd(t)}function tw(t){let n=()=>{ew(t)},e=X0++;t._inner=t._inner.fork({name:"angular",properties:{[Jd]:!0,[uo]:e,[uo+e]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(nw(c))return i.invokeTask(o,s,a,c);try{return pp(t),i.invokeTask(o,s,a,c)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),gp(t)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return pp(t),i.invoke(o,s,a,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!iw(c)&&n(),gp(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,dd(t),eu(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function dd(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function pp(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function gp(t){t._nesting--,eu(t)}var fo=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new F;onMicrotaskEmpty=new F;onStable=new F;onError=new F;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function nw(t){return ig(t,"__ignore_ng_zone__")}function iw(t){return ig(t,"__scheduler_tick__")}function ig(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var At=class{_console=console;handleError(n){this._console.error("ERROR",n)}},xn=new g("",{factory:()=>{let t=f(S),n=f(Ee),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(At),e.handleError(i))})}}}),rg={provide:wi,useValue:()=>{let t=f(At,{optional:!0})},multi:!0},rw=new g("",{factory:()=>{let t=f(P).defaultView;if(!t)return;let n=f(xn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),f(st).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function tu(){return gr([kp(()=>{f(rw)})])}function G(t,n){let[e,i,r]=Nl(t,n?.equal),o=e,s=o[Ce];return o.set=i,o.update=r,o.asReadonly=xo.bind(o),o}function xo(){let t=this[Ce];if(t.readonlyFn===void 0){let n=()=>this();n[Ce]=t,t.readonlyFn=n}return t.readonlyFn}var Cr=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=ow}return t})();function ow(){return new Cr(T(),Le())}var en=class{},wo=new g("",{factory:()=>!0});var nu=new g(""),Er=(()=>{class t{internalPendingTasks=f(Ai);scheduler=f(en);errorHandler=f(xn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=v({token:t,providedIn:"root",factory:()=>new t})}return t})(),Ra=(()=>{class t{static \u0275prov=v({token:t,providedIn:"root",factory:()=>new ud})}return t})(),ud=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},ua=class{[Ce];constructor(n){this[Ce]=n}destroy(){this[Ce].destroy()}};function tt(t,n){let e=n?.injector??f(Z),i=n?.manualCleanup!==!0?e.get(st):null,r,o=e.get(Cr,null,{optional:!0}),s=e.get(en);return o!==null?(r=cw(o.view,s,t),i instanceof la&&i._lView===o.view&&(i=null)):r=lw(t,e.get(Ra),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new ua(r)}var og=B(y({},Fl),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=lo(!1);try{Pl(this)}finally{lo(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=I(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],I(t)}}}),sw=B(y({},og),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Fn(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),aw=B(y({},og),{consumerMarkedDirty(){this.view[A]|=8192,Si(this.view),this.notifier.notify(13)},destroy(){if(Fn(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Pn]?.delete(this)}});function cw(t,n,e){let i=Object.create(aw);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=sg(i,e),t[Pn]??=new Set,t[Pn].add(i),i.consumerMarkedDirty(i),i}function lw(t,n,e){let i=Object.create(sw);return i.fn=sg(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function sg(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Oo(t){return{toString:t}.toString()}function _w(t){return typeof t=="function"}function jg(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Ha=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},Be=(()=>{let t=()=>Hg;return t.ngInherit=!0,t})();function Hg(t){return t.type.prototype.ngOnChanges&&(t.setInput=yw),vw}function vw(){let t=Ug(this),n=t?.current;if(n){let e=t.previous;if(e===Hn)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function yw(t,n,e,i,r){let o=this.declaredInputs[i],s=Ug(t)||xw(t,{previous:Hn,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new Ha(l&&l.currentValue,e,c===Hn),jg(t,n,r,e)}var zg="__ngSimpleChanges__";function Ug(t){return t[zg]||null}function xw(t,n){return t[zg]=n}var ag=[];var ce=function(t,n=null,e){for(let i=0;i<ag.length;i++){let r=ag[i];r(t,n,e)}},oe=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(oe||{});function ww(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=Hg(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function $g(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),c&&(t.viewHooks??=[]).push(-e,c),l&&((t.viewHooks??=[]).push(e,l),(t.viewCheckHooks??=[]).push(e,l)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function La(t,n,e){Wg(t,n,3,e)}function Va(t,n,e,i){(t[A]&3)===e&&Wg(t,n,e,i)}function iu(t,n){let e=t[A];(e&3)===n&&(e&=16383,e+=1,t[A]=e)}function Wg(t,n,e,i){let r=i!==void 0?t[Ei]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(t[Ei]+=65536),(a<o||o==-1)&&(Dw(t,e,n,c),t[Ei]=(t[Ei]&4294901760)+c+2),c++}function cg(t,n){ce(oe.LifecycleHookStart,t,n);let e=I(null);try{n.call(t)}finally{I(e),ce(oe.LifecycleHookEnd,t,n)}}function Dw(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[A]>>14<t[Ei]>>16&&(t[A]&3)===n&&(t[A]+=16384,cg(a,o)):cg(a,o)}var Ir=-1,Oi=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function Cw(t){return(t.flags&8)!==0}function Ew(t){return(t.flags&16)!==0}function kw(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];Iw(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function Gg(t){return t===3||t===4||t===6}function Iw(t){return t.charCodeAt(0)===64}function Mr(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?lg(t,e,r,null,n[++i]):lg(t,e,r,null,null))}}return t}function lg(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function qg(t){return t!==Ir}function za(t){return t&32767}function Mw(t){return t>>16}function Ua(t,n){let e=Mw(t),i=n;for(;e>0;)i=i[Ci],e--;return i}var hu=!0;function dg(t){let n=hu;return hu=t,n}var Sw=256,Zg=Sw-1,Yg=5,Tw=0,on={};function Aw(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(xi)&&(i=e[xi]),i==null&&(i=e[xi]=Tw++);let r=i&Zg,o=1<<r;n.data[t+(r>>Yg)]|=o}function $a(t,n){let e=Qg(t,n);if(e!==-1)return e;let i=n[M];i.firstCreatePass&&(t.injectorIndex=n.length,ru(i.data,t),ru(n,null),ru(i.blueprint,null));let r=Xu(t,n),o=t.injectorIndex;if(qg(r)){let s=za(r),a=Ua(r,n),c=a[M].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function ru(t,n){t.push(0,0,0,0,0,0,0,0,n)}function Qg(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Xu(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=tb(r),i===null)return Ir;if(e++,r=r[Ci],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Ir}function pu(t,n,e){Aw(t,n,e)}function Rw(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Gg(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function Kg(t,n,e){if(e&8||t!==void 0)return t;ba(n,"NodeInjector")}function Xg(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[bn],o=rt(void 0);try{return r?r.get(n,i,e&8):yd(n,i,e&8)}finally{rt(o)}}return Kg(i,n,e)}function Jg(t,n,e,i=0,r){if(t!==null){if(n[A]&2048&&!(i&2)){let s=Pw(t,n,e,i,on);if(s!==on)return s}let o=eb(t,n,e,i,on);if(o!==on)return o}return Xg(n,e,i,r)}function eb(t,n,e,i,r){let o=Nw(e);if(typeof o=="function"){if(!Qd(n,t,i))return i&1?Kg(r,e,i):Xg(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))ba(e);else return s}finally{Kd()}}else if(typeof o=="number"){let s=null,a=Qg(t,n),c=Ir,l=i&1?n[Je][Xe]:null;for((a===-1||i&4)&&(c=a===-1?Xu(t,n):n[a+8],c===Ir||!fg(i,!1)?a=-1:(s=n[M],a=za(c),n=Ua(c,n)));a!==-1;){let d=n[M];if(ug(o,a,d.data)){let u=Ow(a,n,e,s,i,l);if(u!==on)return u}c=n[a+8],c!==Ir&&fg(i,n[M].data[a+8]===l)&&ug(o,a,n)?(s=d,a=za(c),n=Ua(c,n)):a=-1}}return r}function Ow(t,n,e,i,r,o){let s=n[M],a=s.data[t+8],c=i==null?yn(a)&&hu:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=Ba(a,s,e,c,l);return d!==null?ko(n,s,d,a,r):on}function Ba(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,c=t.directiveStart,l=t.directiveEnd,d=o>>20,u=i?a:a+d,m=r?a+d:l;for(let h=u;h<m;h++){let p=s[h];if(h<c&&e===p||h>=c&&p.type===e)return h}if(r){let h=s[c];if(h&&nn(h)&&h.type===e)return c}return null}function ko(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof Oi){let a=o;if(a.resolving)throw vd("");let c=dg(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],d,u=a.injectImpl?rt(a.injectImpl):null,m=Qd(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&ww(e,s[e],n)}finally{u!==null&&rt(u),dg(c),a.resolving=!1,Kd()}}return o}function Nw(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(xi)?t[xi]:void 0;return typeof n=="number"?n>=0?n&Zg:Fw:n}function ug(t,n,e){let i=1<<t;return!!(e[n+(t>>Yg)]&i)}function fg(t,n){return!(t&2)&&!(t&1&&n)}var Ri=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return Jg(this._tNode,this._lView,n,bi(i),e)}};function Fw(){return new Ri(Le(),T())}function an(t){return Oo(()=>{let n=t.prototype.constructor,e=n[ao]||gu(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[ao]||gu(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function gu(t){return fd(t)?()=>{let n=gu(Ue(t));return n&&n()}:_i(t)}function Pw(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[A]&2048&&!wr(s);){let a=eb(o,s,e,i|2,on);if(a!==on)return a;let c=o.parent;if(!c){let l=s[Md];if(l){let d=l.get(e,on,i&-5);if(d!==on)return d}c=tb(s),s=s[Ci]}o=c}return r}function tb(t){let n=t[M],e=n.type;return e===2?n.declTNode:e===1?t[Xe]:null}function Ju(t){return Rw(Le(),t)}function Lw(){return Or(Le(),T())}function Or(t,n){return new O(Ft(t,n))}var O=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=Lw}return t})();function nb(t){return t instanceof O?t.nativeElement:t}function Vw(){return this._results[Symbol.iterator]()}var wn=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new D}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=Dp(n);(this._changesDetected=!wp(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=Vw};function ib(t){return(t.flags&128)===128}var ef=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(ef||{}),rb=new Map,Bw=0;function jw(){return Bw++}function Hw(t){rb.set(t[_n],t)}function bu(t){rb.delete(t[_n])}var mg="__ngContext__";function Sr(t,n){vn(n)?(t[mg]=n[_n],Hw(n)):t[mg]=n}function ob(t){return ab(t[yr])}function sb(t){return ab(t[gt])}function ab(t){for(;t!==null&&!Nt(t);)t=t[gt];return t}var _u;function tf(t){_u=t}function cb(){if(_u!==void 0)return _u;if(typeof document<"u")return document;throw new k(210,!1)}var Pi=new g("",{factory:()=>zw}),zw="ng";var Ja=new g(""),Li=new g("",{providedIn:"platform",factory:()=>"unknown"}),No=new g(""),Vi=new g("",{factory:()=>f(P).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var nf=(()=>{class t{static \u0275prov=v({token:t,providedIn:"root",factory:()=>{let e=new t;return e.store=Uw(f(P),f(Pi)),e}});store={};onSerializeCallbacks={};get(e,i){return this.store[e]!==void 0?this.store[e]:i}set(e,i){this.store[e]=i}remove(e){delete this.store[e]}hasKey(e){return this.store.hasOwnProperty(e)}get isEmpty(){return Object.keys(this.store).length===0}onSerialize(e,i){this.onSerializeCallbacks[e]=i}toJson(){for(let e in this.onSerializeCallbacks)if(this.onSerializeCallbacks.hasOwnProperty(e))try{this.store[e]=this.onSerializeCallbacks[e]()}catch(i){console.warn("Exception in onSerialize callback: ",i)}return JSON.stringify(this.store).replace(/</g,"\\u003C").replace(/\//g,"\\u002F")}}return t})();function Uw(t,n){let e=t.getElementById(n+"-state");if(e?.textContent)try{return JSON.parse(e.textContent)}catch(i){console.warn("Exception while restoring TransferState for app "+n,i)}return{}}var lb="r";var db="di";var ub=!1,fb=new g("",{factory:()=>ub});var hg=new WeakMap;function $w(t,n){if(t==null||typeof t!="object")return;let e=hg.get(t);e||(e=new WeakSet,hg.set(t,e)),e.add(n)}var Ww=(t,n,e,i)=>{};function Gw(t,n,e,i){Ww(t,n,e,i)}function ec(t){return(t.flags&32)===32}var qw=()=>null;function mb(t,n,e=!1){return qw(t,n,e)}function hb(t,n){let e=t.contentQueries;if(e!==null){let i=I(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];_o(o),a.contentQueries(2,n[s],s)}}}finally{I(i)}}}function vu(t,n,e){_o(0);let i=I(null);try{n(t,e)}finally{I(i)}}function rf(t,n,e){if(Td(n)){let i=I(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{I(i)}}}var Ht=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Ht||{});var Oa;function Zw(){if(Oa===void 0&&(Oa=null,Bn.trustedTypes))try{Oa=Bn.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Oa}function tc(t){return Zw()?.createHTML(t)||t}var Na;function Yw(){if(Na===void 0&&(Na=null,Bn.trustedTypes))try{Na=Bn.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Na}function pg(t){return Yw()?.createHTML(t)||t}var Dn=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${fa})`}},yu=class extends Dn{getTypeName(){return"HTML"}},xu=class extends Dn{getTypeName(){return"Style"}},wu=class extends Dn{getTypeName(){return"Script"}},Du=class extends Dn{getTypeName(){return"URL"}},Cu=class extends Dn{getTypeName(){return"ResourceURL"}};function zt(t){return t instanceof Dn?t.changingThisBreaksApplicationSecurity:t}function Cn(t,n){let e=pb(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${fa})`)}return e===n}function pb(t){return t instanceof Dn&&t.getTypeName()||null}function of(t){return new yu(t)}function sf(t){return new xu(t)}function af(t){return new wu(t)}function cf(t){return new Du(t)}function lf(t){return new Cu(t)}function Qw(t){let n=new ku(t);return Kw()?new Eu(n):n}var Eu=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(tc(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},ku=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=tc(n),e}};function Kw(){try{return!!new window.DOMParser().parseFromString(tc(""),"text/html")}catch{return!1}}var Xw=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Fo(t){return t=String(t),t.match(Xw)?t:"unsafe:"+t}function En(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Po(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var gb=En("area,br,col,hr,img,wbr"),bb=En("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),_b=En("rp,rt"),Jw=Po(_b,bb),eD=Po(bb,En("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),tD=Po(_b,En("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),gg=Po(gb,eD,tD,Jw),vb=En("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),nD=En("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),iD=En("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),rD=Po(vb,nD,iD),oD=En("script,style,template");var Iu=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=cD(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=aD(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=bg(n).toLowerCase();if(!gg.hasOwnProperty(e))return this.sanitizedSomething=!0,!oD.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!rD.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;vb[a]&&(c=Fo(c)),this.buf.push(" ",s,'="',_g(c),'"')}return this.buf.push(">"),!0}endElement(n){let e=bg(n).toLowerCase();gg.hasOwnProperty(e)&&!gb.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(_g(n))}};function sD(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function aD(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw yb(n);return n}function cD(t){let n=t.firstChild;if(n&&sD(t,n))throw yb(n);return n}function bg(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function yb(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var lD=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,dD=/([^\#-~ |!])/g;function _g(t){return t.replace(/&/g,"&amp;").replace(lD,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(dD,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Fa;function nc(t,n){let e=null;try{Fa=Fa||Qw(t);let i=n?String(n):"";e=Fa.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Fa.getInertBodyElement(i)}while(i!==o);let a=new Iu().sanitizeChildren(vg(e)||e);return tc(a)}finally{if(e){let i=vg(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function vg(t){return"content"in t&&uD(t)?t.content:null}function uD(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var fD=/^>|^->|<!--|-->|--!>|<!-$/g,mD=/(<|>)/g,hD="\u200B$1\u200B";function pD(t){return t.replace(fD,n=>n.replace(mD,hD))}function gD(t,n){return t.createText(n)}function bD(t,n,e){t.setValue(n,e)}function _D(t,n){return t.createComment(pD(n))}function xb(t,n,e){return t.createElement(n,e)}function Wa(t,n,e,i,r){t.insertBefore(n,e,i,r)}function wb(t,n,e){t.appendChild(n,e)}function yg(t,n,e,i,r){i!==null?Wa(t,n,e,i,r):wb(t,n,e)}function Db(t,n,e,i){t.removeChild(null,n,e,i)}function vD(t,n,e){t.setAttribute(n,"style",e)}function yD(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function Cb(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&kw(t,n,i),r!==null&&yD(t,n,r),o!==null&&vD(t,n,o)}var at=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t})(at||{});function Bi(t){let n=Eb();return n?pg(n.sanitize(at.HTML,t)||""):Cn(t,"HTML")?pg(zt(t)):nc(cb(),mo(t))}function df(t){let n=Eb();return n?n.sanitize(at.URL,t)||"":Cn(t,"URL")?zt(t):Fo(mo(t))}function Eb(){let t=T();return t&&t[Ot].sanitizer}function xD(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var kb="ng-template";function wD(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&xD(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(uf(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function uf(t){return t.type===4&&t.value!==kb}function DD(t,n,e){let i=t.type===4&&!e?kb:t.value;return n===i}function CD(t,n,e){let i=4,r=t.attrs,o=r!==null?ID(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!Bt(i)&&!Bt(c))return!1;if(s&&Bt(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!DD(t,c,e)||c===""&&n.length===1){if(Bt(i))return!1;s=!0}}else if(i&8){if(r===null||!wD(t,r,c,e)){if(Bt(i))return!1;s=!0}}else{let l=n[++a],d=ED(c,r,uf(t),e);if(d===-1){if(Bt(i))return!1;s=!0;continue}if(l!==""){let u;if(d>o?u="":u=r[d+1].toLowerCase(),i&2&&l!==u){if(Bt(i))return!1;s=!0}}}}return Bt(i)||s}function Bt(t){return(t&1)===0}function ED(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return MD(n,t)}function Ib(t,n,e=!1){for(let i=0;i<n.length;i++)if(CD(t,n[i],e))return!0;return!1}function kD(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function ID(t){for(let n=0;n<t.length;n++){let e=t[n];if(Gg(e))return n}return t.length}function MD(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function SD(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function xg(t,n){return t?":not("+n.trim()+")":n}function TD(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!Bt(s)&&(n+=xg(o,r),r=""),i=s,o=o||!Bt(i);e++}return r!==""&&(n+=xg(o,r)),n}function AD(t){return t.map(TD).join(",")}function RD(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!Bt(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var ct={};function ff(t,n,e,i,r,o,s,a,c,l,d){let u=be+i,m=u+r,h=OD(u,m),p=typeof l=="function"?l():l;return h[M]={type:t,blueprint:h,template:e,queries:null,viewQuery:a,declTNode:n,data:h.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:m,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:p,incompleteFirstPass:!1,ssrId:d}}function OD(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:ct);return e}function ND(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=ff(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function mf(t,n,e,i,r,o,s,a,c,l,d){let u=n.blueprint.slice();return u[Rt]=r,u[A]=i|4|128|8|64|1024,(l!==null||t&&t[A]&2048)&&(u[A]|=2048),Nd(u),u[Oe]=u[Ci]=t,u[ke]=e,u[Ot]=s||t&&t[Ot],u[ue]=a||t&&t[ue],u[bn]=c||t&&t[bn]||null,u[Xe]=o,u[_n]=jw(),u[Di]=d,u[Md]=l,u[Je]=n.type==2?t[Je]:u,u}function FD(t,n,e){let i=Ft(n,t),r=ND(e),o=t[Ot].rendererFactory,s=hf(t,mf(t,r,null,Mb(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function Mb(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function Sb(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function hf(t,n){return t[yr]?t[Id][gt]=n:t[yr]=n,t[Id]=n,n}function x(t=1){Tb(he(),T(),rn()+t,!1)}function Tb(t,n,e,i){if(!i)if((n[A]&3)===3){let o=t.preOrderCheckHooks;o!==null&&La(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Va(n,o,0,e)}Gn(e)}var ic=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(ic||{});function Mu(t,n,e,i){let r=I(null);try{let[o,s,a]=t.inputs[e],c=null;(s&ic.SignalBased)!==0&&(c=n[o][Ce]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,c,i,e,o):jg(n,c,o,i)}finally{I(r)}}var sn=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(sn||{}),PD;function pf(t,n){return PD(t,n)}var _V=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Su=new WeakMap,Do=new WeakSet;function LD(t,n){let e=Su.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let s=e[o],a=s.parentNode;s===n?(e.splice(o,1),Do.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(e.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function VD(t,n){let e=Su.get(t);e?e.includes(n)||e.push(n):Su.set(t,[n])}var Ni=new Set,rc=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(rc||{}),Ut=new g(""),wg=new Set;function kn(t){wg.has(t)||(wg.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var oc=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=v({token:t,providedIn:"root",factory:()=>new t})}return t})(),gf=[0,1,2,3],bf=(()=>{class t{ngZone=f(S);scheduler=f(en);errorHandler=f(At,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){f(Ut,{optional:!0})}execute(){let e=this.sequences.size>0;e&&ce(oe.AfterRenderHooksStart),this.executing=!0;for(let i of gf)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&ce(oe.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[ki]??=[]).push(e),Si(i),i[A]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(rc.AFTER_NEXT_RENDER,e):e()}static \u0275prov=v({token:t,providedIn:"root",factory:()=>new t})}return t})(),Io=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[ki];n&&(this.view[ki]=n.filter(e=>e!==this))}};function cn(t,n){let e=n?.injector??f(Z);return kn("NgAfterNextRender"),jD(t,e,n,!0)}function BD(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function jD(t,n,e,i){let r=n.get(oc);r.impl??=n.get(bf);let o=n.get(Ut,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(st):null,a=n.get(Cr,null,{optional:!0}),c=new Io(r.impl,BD(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var Ab=new g("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:f(Ee)})});function Rb(t,n,e){let i=t.get(Ab);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function HD(t,n){let e=t.get(Ab);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function zD(t,n){for(let[e,i]of n)Rb(t,i.animateFns)}function Dg(t,n,e,i){let r=t?.[Un]?.enter;n!==null&&r&&r.has(e.index)&&zD(i,r)}function kr(t,n,e,i,r,o,s,a){if(r!=null){let c,l=!1;Nt(r)?c=r:vn(r)&&(l=!0,r=r[Rt]);let d=bt(r);t===0&&i!==null?(Dg(a,i,o,e),s==null?wb(n,i,d):Wa(n,i,d,s||null,!0)):t===1&&i!==null?(Dg(a,i,o,e),Wa(n,i,d,s||null,!0),LD(o,d)):t===2?(a?.[Un]?.leave?.has(o.index)&&VD(o,d),Do.delete(d),Cg(a,o,e,u=>{if(Do.has(d)){Do.delete(d);return}Db(n,d,l,u)})):t===3&&(Do.delete(d),Cg(a,o,e,()=>{n.destroyNode(d)})),c!=null&&JD(n,t,e,c,o,i,s)}}function UD(t,n){Ob(t,n),n[Rt]=null,n[Xe]=null}function $D(t,n,e,i,r,o){i[Rt]=r,i[Xe]=n,ac(t,i,e,1,r,o)}function Ob(t,n){n[Ot].changeDetectionScheduler?.notify(9),ac(t,n,n[ue],2,null,null)}function WD(t){let n=t[yr];if(!n)return ou(t[M],t);for(;n;){let e=null;if(vn(n))e=n[yr];else{let i=n[we];i&&(e=i)}if(!e){for(;n&&!n[gt]&&n!==t;)vn(n)&&ou(n[M],n),n=n[Oe];n===null&&(n=t),vn(n)&&ou(n[M],n),e=n&&n[gt]}n=e}}function _f(t,n){let e=t[Ii],i=e.indexOf(n);e.splice(i,1)}function sc(t,n){if(Mi(n))return;let e=n[ue];e.destroyNode&&ac(t,n,e,3,null,null),WD(n)}function ou(t,n){if(Mi(n))return;let e=I(null);try{n[A]&=-129,n[A]|=256,n[ot]&&Fn(n[ot]),ZD(t,n),qD(t,n),n[M].type===1&&n[ue].destroy();let i=n[zn];if(i!==null&&Nt(n[Oe])){i!==n[Oe]&&_f(i,n);let r=n[tn];r!==null&&r.detachView(t)}bu(n)}finally{I(e)}}function Cg(t,n,e,i){let r=t?.[Un];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&Ni.add(t[_n]),Rb(e,()=>{if(r.leave&&r.leave.has(n.index)){let s=r.leave.get(n.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:d}=l();a.push(d)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),GD(t,i)}else t&&Ni.delete(t[_n]),i(!1)},r)}function GD(t,n){let e=t[Un]?.running;if(e){e.then(()=>{t[Un].running=void 0,Ni.delete(t[_n]),n(!0)});return}n(!1)}function qD(t,n){let e=t.cleanup,i=n[vr];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[vr]=null);let r=n[gn];if(r!==null){n[gn]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Pn];if(o!==null){n[Pn]=null;for(let s of o)s.destroy()}}function ZD(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Oi)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];ce(oe.LifecycleHookStart,a,c);try{c.call(a)}finally{ce(oe.LifecycleHookEnd,a,c)}}else{ce(oe.LifecycleHookStart,r,o);try{o.call(r)}finally{ce(oe.LifecycleHookEnd,r,o)}}}}}function Nb(t,n,e){return YD(t,n.parent,e)}function YD(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[Rt];if(yn(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Ht.None||r===Ht.Emulated)return null}return Ft(i,e)}function Fb(t,n,e){return KD(t,n,e)}function QD(t,n,e){return t.type&40?Ft(t,e):null}var KD=QD,Eg;function vf(t,n,e,i){let r=Nb(t,i,n),o=n[ue],s=i.parent||n[Xe],a=Fb(s,i,n);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)yg(o,r,e[c],a,!1);else yg(o,r,e,a,!1);Eg!==void 0&&Eg(o,i,n,e,r)}function Co(t,n){if(n!==null){let e=n.type;if(e&3)return Ft(n,t);if(e&4)return Tu(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Co(t,i);{let r=t[n.index];return Nt(r)?Tu(-1,r):bt(r)}}else{if(e&128)return Co(t,n.next);if(e&32)return pf(n,t)()||bt(t[n.index]);{let i=Pb(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=Ln(t[Je]);return Co(r,i)}else return Co(t,n.next)}}}return null}function Pb(t,n){if(n!==null){let i=t[Je][Xe],r=n.projection;return i.projection[r]}return null}function Tu(t,n){let e=we+t+1;if(e<n.length){let i=n[e],r=i[M].firstChild;if(r!==null)return Co(i,r)}return n[$n]}function yf(t,n,e,i,r,o,s){for(;e!=null;){let a=i[bn];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(s&&n===0&&(c&&Sr(bt(c),i),e.flags|=2),!ec(e))if(l&8)yf(t,n,e.child,i,r,o,!1),kr(n,t,a,r,c,e,o,i);else if(l&32){let d=pf(e,i),u;for(;u=d();)kr(n,t,a,r,u,e,o,i);kr(n,t,a,r,c,e,o,i)}else l&16?Lb(t,n,i,e,r,o):kr(n,t,a,r,c,e,o,i);e=s?e.projectionNext:e.next}}function ac(t,n,e,i,r,o){yf(e,i,t.firstChild,n,r,o,!1)}function XD(t,n,e){let i=n[ue],r=Nb(t,e,n),o=e.parent||n[Xe],s=Fb(o,e,n);Lb(i,0,n,e,r,s)}function Lb(t,n,e,i,r,o){let s=e[Je],c=s[Xe].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];kr(n,t,e[bn],r,d,i,o,e)}else{let l=c,d=s[Oe];ib(i)&&(l.flags|=128),yf(t,n,l,d,r,o,!0)}}function JD(t,n,e,i,r,o,s){let a=i[$n],c=bt(i);a!==c&&kr(n,t,e,o,a,r,s);for(let l=we;l<i.length;l++){let d=i[l];ac(d[M],d,t,n,o,a)}}function eC(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:sn.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=sn.Important),t.setStyle(e,i,r,o))}}function Vb(t,n,e,i,r){let o=rn(),s=i&2;try{Gn(-1),s&&n.length>be&&Tb(t,n,be,!1);let a=s?oe.TemplateUpdateStart:oe.TemplateCreateStart;ce(a,r,e),e(i,r)}finally{Gn(o);let a=s?oe.TemplateUpdateEnd:oe.TemplateCreateEnd;ce(a,r,e)}}function cc(t,n,e){sC(t,n,e),(e.flags&64)===64&&aC(t,n,e)}function Lo(t,n,e=Ft){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function tC(t,n,e,i){let o=i.get(fb,ub)||e===Ht.ShadowDom||e===Ht.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return nC(s),s}function nC(t){iC(t)}var iC=()=>null;function rC(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function Bb(t,n,e,i,r,o){let s=n[M];if(Cf(t,s,n,e,i)){yn(t)&&oC(n,t.index);return}t.type&3&&(e=rC(e)),jb(t,n,e,i,r,o)}function jb(t,n,e,i,r,o){if(t.type&3){let s=Ft(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function oC(t,n){let e=_t(n,t);e[A]&16||(e[A]|=64)}function sC(t,n,e){let i=e.directiveStart,r=e.directiveEnd;yn(e)&&FD(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||$a(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],c=ko(n,t,s,e);if(Sr(c,n),o!==null&&uC(n,s-i,c,a,e,o),nn(a)){let l=_t(e.index,n);l[ke]=ko(n,t,s,e)}}}function aC(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=Wp();try{Gn(o);for(let a=i;a<r;a++){let c=t.data[a],l=n[a];Ia(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&cC(c,l)}}finally{Gn(-1),Ia(s)}}function cC(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function xf(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];Ib(n,o.selectors,!1)&&(i??=[],nn(o)?i.unshift(o):i.push(o))}return i}function lC(t,n,e,i,r,o){let s=Ft(t,n);dC(n[ue],s,o,t.value,e,i,r)}function dC(t,n,e,i,r,o,s){if(o==null)t.removeAttribute(n,r,e);else{let a=s==null?mo(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function uC(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Mu(i,e,c,l)}}function wf(t,n,e,i,r){let o=be+e,s=n[M],a=r(s,n,t,i,e);n[o]=a,Ti(t,!0);let c=t.type===2;return c?(Cb(n[ue],a,t),(Bp()===0||xr(t))&&Sr(a,n),jp()):Sr(a,n),Aa()&&(!c||!ec(t))&&vf(s,n,a,t),t}function Df(t){let n=t;return $d()?Wd():(n=n.parent,Ti(n,!1)),n}function fC(t,n){let e=t[bn];if(!e)return;let i;try{i=e.get(xn,null)}catch{i=null}i?.(n)}function Cf(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],u=n.data[l];Mu(u,e[l],d,r),a=!0}if(o)for(let c of o){let l=e[c],d=n.data[c];Mu(d,l,i,r),a=!0}return a}function mC(t,n){let e=_t(n,t),i=e[M];hC(i,e);let r=e[Rt];r!==null&&e[Di]===null&&(e[Di]=mb(r,e[bn])),ce(oe.ComponentStart);try{Ef(i,e,e[ke])}finally{ce(oe.ComponentEnd,e[ke])}}function hC(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function Ef(t,n,e){Sa(n);try{let i=t.viewQuery;i!==null&&vu(1,i,e);let r=t.template;r!==null&&Vb(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[tn]?.finishViewCreation(t),t.staticContentQueries&&hb(t,n),t.staticViewQueries&&vu(2,t.viewQuery,e);let o=t.components;o!==null&&pC(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[A]&=-5,Ta()}}function pC(t,n){for(let e=0;e<n.length;e++)mC(t,n[e])}function Vo(t,n,e,i){let r=I(null);try{let o=n.tView,a=t[A]&4096?4096:16,c=mf(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=t[n.index];c[zn]=l;let d=t[tn];return d!==null&&(c[tn]=d.createEmbeddedView(o)),Ef(o,c,e),c}finally{I(r)}}function Tr(t,n){return!n||n.firstChild===null||ib(t)}function Mo(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(bt(o)),Nt(o)&&Hb(o,i);let s=e.type;if(s&8)Mo(t,n,e.child,i);else if(s&32){let a=pf(e,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=Pb(n,e);if(Array.isArray(a))i.push(...a);else{let c=Ln(n[Je]);Mo(c[M],c,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function Hb(t,n){for(let e=we;e<t.length;e++){let i=t[e],r=i[M].firstChild;r!==null&&Mo(i[M],i,r,n)}t[$n]!==t[Rt]&&n.push(t[$n])}function zb(t){if(t[ki]!==null){for(let n of t[ki])n.impl.addSequence(n);t[ki].length=0}}var Ub=[];function gC(t){return t[ot]??bC(t)}function bC(t){let n=Ub.pop()??Object.create(vC);return n.lView=t,n}function _C(t){t.lView[ot]!==t&&(t.lView=null,Ub.push(t))}var vC=B(y({},Rn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Si(t.lView)},consumerOnSignalRead(){this.lView[ot]=this}});function yC(t){let n=t[ot]??Object.create(xC);return n.lView=t,n}var xC=B(y({},Rn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=Ln(t.lView);for(;n&&!$b(n[M]);)n=Ln(n);n&&Fd(n)},consumerOnSignalRead(){this.lView[ot]=this}});function $b(t){return t.type!==2}function Wb(t){if(t[Pn]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Pn])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[A]&8192)}}var wC=100;function Gb(t,n=0){let i=t[Ot].rendererFactory,r=!1;r||i.begin?.();try{DC(t,n)}finally{r||i.end?.()}}function DC(t,n){let e=qd();try{lo(!0),Au(t,n);let i=0;for(;bo(t);){if(i===wC)throw new k(103,!1);i++,Au(t,1)}}finally{lo(e)}}function CC(t,n,e,i){if(Mi(n))return;let r=n[A],o=!1,s=!1;Sa(n);let a=!0,c=null,l=null;o||($b(t)?(l=gC(n),c=mn(l)):Es()===null?(a=!1,l=yC(n),c=mn(l)):n[ot]&&(Fn(n[ot]),n[ot]=null));try{Nd(n),zp(t.bindingStartIndex),e!==null&&Vb(t,n,e,2,i);let d=(r&3)===3;if(!o)if(d){let h=t.preOrderCheckHooks;h!==null&&La(n,h,null)}else{let h=t.preOrderHooks;h!==null&&Va(n,h,0,null),iu(n,0)}if(s||EC(n),Wb(n),qb(n,0),t.contentQueries!==null&&hb(t,n),!o)if(d){let h=t.contentCheckHooks;h!==null&&La(n,h)}else{let h=t.contentHooks;h!==null&&Va(n,h,1),iu(n,1)}IC(t,n);let u=t.components;u!==null&&Yb(n,u,0);let m=t.viewQuery;if(m!==null&&vu(2,m,i),!o)if(d){let h=t.viewCheckHooks;h!==null&&La(n,h)}else{let h=t.viewHooks;h!==null&&Va(n,h,2),iu(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[xa]){for(let h of n[xa])h();n[xa]=null}o||(zb(n),n[A]&=-73)}catch(d){throw o||Si(n),d}finally{l!==null&&(Nn(l,c),a&&_C(l)),Ta()}}function qb(t,n){for(let e=ob(t);e!==null;e=sb(e))for(let i=we;i<e.length;i++){let r=e[i];Zb(r,n)}}function EC(t){for(let n=ob(t);n!==null;n=sb(n)){if(!(n[A]&2))continue;let e=n[Ii];for(let i=0;i<e.length;i++){let r=e[i];Fd(r)}}}function kC(t,n,e){ce(oe.ComponentStart);let i=_t(n,t);try{Zb(i,e)}finally{ce(oe.ComponentEnd,i[ke])}}function Zb(t,n){Ca(t)&&Au(t,n)}function Au(t,n){let i=t[M],r=t[A],o=t[ot],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&or(o)),s||=!1,o&&(o.dirty=!1),t[A]&=-9217,s)CC(i,t,i.template,t[ke]);else if(r&8192){let a=I(null);try{Wb(t),qb(t,1);let c=i.components;c!==null&&Yb(t,c,1),zb(t)}finally{I(a)}}}function Yb(t,n,e){for(let i=0;i<n.length;i++)kC(t,n[i],e)}function IC(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Gn(~r);else{let o=r,s=e[++i],a=e[++i];$p(s,o);let c=n[o];ce(oe.HostBindingsUpdateStart,c);try{a(2,c)}finally{ce(oe.HostBindingsUpdateEnd,c)}}}}finally{Gn(-1)}}function kf(t,n){let e=qd()?64:1088;for(t[Ot].changeDetectionScheduler?.notify(n);t;){t[A]|=e;let i=Ln(t);if(wr(t)&&!i)return t;t=i}return null}function Qb(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function Kb(t,n){let e=we+n;if(e<t.length)return t[e]}function Bo(t,n,e,i=!0){let r=n[M];if(MC(r,n,t,e),i){let s=Tu(e,t),a=n[ue],c=a.parentNode(t[$n]);c!==null&&$D(r,t[Xe],a,n,c,s)}let o=n[Di];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function Xb(t,n){let e=So(t,n);return e!==void 0&&sc(e[M],e),e}function So(t,n){if(t.length<=we)return;let e=we+n,i=t[e];if(i){let r=i[zn];r!==null&&r!==t&&_f(r,i),n>0&&(t[e-1][gt]=i[gt]);let o=ho(t,we+n);UD(i[M],i);let s=o[tn];s!==null&&s.detachView(o[M]),i[Oe]=null,i[gt]=null,i[A]&=-129}return i}function MC(t,n,e,i){let r=we+i,o=e.length;i>0&&(e[r-1][gt]=n),i<o-we?(n[gt]=e[r],xd(e,we+i,n)):(e.push(n),n[gt]=null),n[Oe]=e;let s=n[zn];s!==null&&e!==s&&Jb(s,n);let a=n[tn];a!==null&&a.insertView(t),Ea(n),n[A]|=128}function Jb(t,n){let e=t[Ii],i=n[Oe];if(vn(i))t[A]|=2;else{let r=i[Oe][Je];n[Je]!==r&&(t[A]|=2)}e===null?t[Ii]=[n]:e.push(n)}var qn=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[M];return Mo(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[ke]}set context(n){this._lView[ke]=n}get destroyed(){return Mi(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Oe];if(Nt(n)){let e=n[go],i=e?e.indexOf(this):-1;i>-1&&(So(n,i),ho(e,i))}this._attachedToViewContainer=!1}sc(this._lView[M],this._lView)}onDestroy(n){Pd(this._lView,n)}markForCheck(){kf(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[A]&=-129}reattach(){Ea(this._lView),this._lView[A]|=128}detectChanges(){this._lView[A]|=1024,Gb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new k(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=wr(this._lView),e=this._lView[zn];e!==null&&!n&&_f(e,this._lView),Ob(this._lView[M],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new k(902,!1);this._appRef=n;let e=wr(this._lView),i=this._lView[zn];i!==null&&!e&&Jb(i,this._lView),Ea(this._lView)}};var nt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=SC;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Vo(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new qn(o)}}return t})();function SC(){return lc(Le(),T())}function lc(t,n){return t.type&4?new nt(n,t,Or(t,n)):null}function ji(t,n,e,i,r){let o=t.data[n];if(o===null)o=TC(t,n,e,i,r),Up()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=Hp();o.injectorIndex=s===null?-1:s.injectorIndex}return Ti(o,!0),o}function TC(t,n,e,i,r){let o=Ud(),s=$d(),a=s?o:o&&o.parent,c=t.data[n]=RC(t,a,e,n,i,r);return AC(t,c,o,s),c}function AC(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function RC(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return jd()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function OC(t){let n=t[Sd]??[],i=t[Oe][ue],r=[];for(let o of n)o.data[db]!==void 0?r.push(o):NC(o,i);t[Sd]=r}function NC(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[lb];for(;e<r;){let o=i.nextSibling;Db(n,i,!1),i=o,e++}}}var FC=()=>null,PC=()=>null;function Ga(t,n){return FC(t,n)}function e_(t,n,e){return PC(t,n,e)}var t_=class{},dc=class{},Ru=class{resolveComponentFactory(n){throw new k(917,!1)}},uc=class{static NULL=new Ru},Ve=class{},qe=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>LC()}return t})();function LC(){let t=T(),n=Le(),e=_t(n.index,t);return(vn(e)?e:t)[ue]}var n_=(()=>{class t{static \u0275prov=v({token:t,providedIn:"root",factory:()=>null})}return t})();var ja={},Ou=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,ja,i);return r!==ja||e===ja?r:this.parentInjector.get(n,e,i)}};function qa(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=ha(r,a);else if(o==2){let c=a,l=n[++s];i=ha(i,c+": "+l+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function pe(t,n=0){let e=T();if(e===null)return R(t,n);let i=Le();return Jg(i,e,Ue(t),n)}function i_(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}jC(t,n,e,a,o,c,l)}o!==null&&i!==null&&VC(e,i,o)}function VC(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new k(-301,!1);i.push(n[r],o)}}function BC(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function jC(t,n,e,i,r,o,s){let a=i.length,c=null;for(let m=0;m<a;m++){let h=i[m];c===null&&nn(h)&&(c=h,BC(t,e,m)),pu($a(e,n),t,h.type)}GC(e,t.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let m=0;m<a;m++){let h=i[m];h.providersResolver&&h.providersResolver(h)}let l=!1,d=!1,u=Sb(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let m=0;m<a;m++){let h=i[m];if(e.mergedAttrs=Mr(e.mergedAttrs,h.hostAttrs),zC(t,e,n,u,h),WC(u,h,r),s!==null&&s.has(h)){let[b,_]=s.get(h);e.directiveToIndex.set(h.type,[u,b+e.directiveStart,_+e.directiveStart])}else(o===null||!o.has(h))&&e.directiveToIndex.set(h.type,u);h.contentQueries!==null&&(e.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(e.flags|=64);let p=h.type.prototype;!l&&(p.ngOnChanges||p.ngOnInit||p.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),l=!0),!d&&(p.ngOnChanges||p.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),u++}HC(t,e,o)}function HC(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))kg(0,n,r,i),kg(1,n,r,i),Mg(n,i,!1);else{let o=e.get(r);Ig(0,n,o,i),Ig(1,n,o,i),Mg(n,i,!0)}}}function kg(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),r_(n,o)}}function Ig(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),r_(n,s)}}function r_(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function Mg(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||uf(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===n){s??=[],s.push(c,i[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function zC(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=_i(r.type,!0)),s=new Oi(o,nn(r),pe,null);t.blueprint[i]=s,e[i]=s,UC(t,n,i,Sb(t,e,r.hostVars,ct),r)}function UC(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;$C(s)!=a&&s.push(a),s.push(e,i,o)}}function $C(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function WC(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;nn(n)&&(e[""]=t)}}function GC(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function If(t,n,e,i,r,o,s,a){let c=n[M],l=c.consts,d=vt(l,s),u=ji(c,t,e,i,d);return o&&i_(c,n,u,vt(l,a),r),u.mergedAttrs=Mr(u.mergedAttrs,u.attrs),u.attrs!==null&&qa(u,u.attrs,!1),u.mergedAttrs!==null&&qa(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function Mf(t,n){$g(t,n),Td(n)&&t.queries.elementEnd(n)}function qC(t,n,e,i,r,o){let s=n.consts,a=vt(s,r),c=ji(n,t,e,i,a);if(c.mergedAttrs=Mr(c.mergedAttrs,c.attrs),o!=null){let l=vt(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&qa(c,c.attrs,!1),c.mergedAttrs!==null&&qa(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}function o_(t,n,e){return t[n]=e}function ZC(t,n){return t[n]}function ln(t,n,e){if(e===ct)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function su(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&$w(r,o);let s=yn(t)?_t(t.index,n):n;kf(s,5);let a=n[ke],c=Sg(n,a,e,r),l=i.__ngNextListenerFn__;for(;l;)c=Sg(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function Sg(t,n,e,i){let r=I(null);try{return ce(oe.OutputStart,n,e),e(i)!==!1}catch(o){return fC(t,o),!1}finally{ce(oe.OutputEnd,n,e),I(r)}}function YC(t,n,e,i,r,o,s,a){let c=xr(t),l=!1,d=null;if(!i&&c&&(d=KC(n,e,o,t.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let u=Ft(t,e),m=i?i(u):u;Gw(e,m,o,a),i||(a.__ngNativeEl__=u);let h=r.listen(m,o,a);if(!QC(o)){let p=i?b=>i(bt(b[t.index])):t.index;s_(p,n,e,o,a,h,!1)}}return l}function QC(t){return t.startsWith("animation")||t.startsWith("transition")}function KC(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[vr],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function s_(t,n,e,i,r,o,s){let a=n.firstCreatePass?Vd(n):null,c=Ld(e),l=c.length;c.push(r,o),a&&a.push(i,t,l,(l+1)*(s?-1:1))}function Tg(t,n,e,i,r,o){let s=n[e],a=n[M],l=a.data[e].outputs[i],u=s[l].subscribe(o);s_(t.index,a,n,r,o,u,!0)}var Nu=Symbol("BINDING");function a_(t){return t.debugInfo?.className||t.type.name||null}var Fu=class extends uc{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=jn(n);return new Ar(e,this.ngModule)}};function XC(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&ic.SignalBased)!==0};return r&&(o.transform=r),o})}function JC(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function eE(t,n,e){let i=n instanceof Ee?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new Ou(e,i):e}function tE(t){let n=t.get(Ve,null);if(n===null)throw new k(407,!1);let e=t.get(n_,null),i=t.get(en,null),r=t.get(Ut,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function nE(t,n){let e=c_(t);return xb(n,e,e==="svg"?Ad:e==="math"?Op:null)}function c_(t){return(t.selectors[0][0]||"div").toLowerCase()}var Ar=class extends dc{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=XC(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=JC(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=AD(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){ce(oe.DynamicComponentStart);let a=I(null);try{let c=this.componentDef,l=eE(c,r||this.ngModule,n),d=tE(l),u=d.tracingService;return u&&u.componentCreate?u.componentCreate(a_(c),()=>this.createComponentRef(d,l,e,i,o,s)):this.createComponentRef(d,l,e,i,o,s)}finally{I(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,c=iE(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=r?tC(l,r,a.encapsulation,e):nE(a,l),u=s?.some(Ag)||o?.some(p=>typeof p!="function"&&p.bindings.some(Ag)),m=mf(null,c,null,512|Mb(a),null,null,n,l,e,null,mb(d,e,!0));m[be]=d,Sa(m);let h=null;try{let p=If(be,m,2,"#host",()=>c.directiveRegistry,!0,0);Cb(l,d,p),Sr(d,m),cc(c,m,p),rf(c,p,m),Mf(c,p),i!==void 0&&oE(p,this.ngContentSelectors,i),h=_t(p.index,m),m[ke]=h[ke],Ef(c,m,null)}catch(p){throw h!==null&&bu(h),bu(m),p}finally{ce(oe.DynamicComponentEnd),Ta()}return new Za(this.componentType,m,!!u)}};function iE(t,n,e,i){let r=t?["ng-version","21.2.12"]:RD(n.selectors[0]),o=null,s=null,a=0;if(e)for(let d of e)a+=d[Nu].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let u=i[d];if(typeof u!="function")for(let m of u.bindings){a+=m[Nu].requiredVars;let h=d+1;m.create&&(m.targetIdx=h,(o??=[]).push(m)),m.update&&(m.targetIdx=h,(s??=[]).push(m))}}let c=[n];if(i)for(let d of i){let u=typeof d=="function"?d:d.type,m=bd(u);c.push(m)}return ff(0,null,rE(o,s),1,a,c,null,null,null,[r],null)}function rE(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function Ag(t){let n=t[Nu].kind;return n==="input"||n==="twoWay"}var Za=class extends t_{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=wa(e[M],be),this.location=Or(this._tNode,e),this.instance=_t(this._tNode.index,e)[ke],this.hostView=this.changeDetectorRef=new qn(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=Cf(i,r[M],r,n,e);this.previousInputValues.set(n,e);let s=_t(i.index,r);kf(s,1)}get injector(){return new Ri(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function oE(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var lt=(()=>{class t{static __NG_ELEMENT_ID__=sE}return t})();function sE(){let t=Le();return l_(t,T())}var Pu=class t extends lt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Or(this._hostTNode,this._hostLView)}get injector(){return new Ri(this._hostTNode,this._hostLView)}get parentInjector(){let n=Xu(this._hostTNode,this._hostLView);if(qg(n)){let e=Ua(n,this._hostLView),i=za(n),r=e[M].data[i+8];return new Ri(r,e)}else return new Ri(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Rg(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-we}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=Ga(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,Tr(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let c=n&&!_w(n),l;if(c)l=e;else{let _=e||{};l=_.index,i=_.injector,r=_.projectableNodes,o=_.environmentInjector||_.ngModuleRef,s=_.directives,a=_.bindings}let d=c?n:new Ar(jn(n)),u=i||this.parentInjector;if(!o&&d.ngModule==null){let E=(c?u:this.parentInjector).get(Ee,null);E&&(o=E)}let m=jn(d.componentType??{}),h=Ga(this._lContainer,m?.id??null),p=h?.firstChild??null,b=d.create(u,r,p,o,s,a);return this.insertImpl(b.hostView,l,Tr(this._hostTNode,h)),b}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(Fp(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[Oe],l=new t(c,c[Xe],c[Oe]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Bo(s,r,o,i),n.attachToViewContainerRef(),xd(au(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Rg(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=So(this._lContainer,e);i&&(ho(au(this._lContainer),e),sc(i[M],i))}detach(n){let e=this._adjustIndex(n,-1),i=So(this._lContainer,e);return i&&ho(au(this._lContainer),e)!=null?new qn(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function Rg(t){return t[go]}function au(t){return t[go]||(t[go]=[])}function l_(t,n){let e,i=n[t.index];return Nt(i)?e=i:(e=Qb(i,n,null,t),n[t.index]=e,hf(n,e)),cE(e,n,t,i),new Pu(e,t,n)}function aE(t,n){let e=t[ue],i=e.createComment(""),r=Ft(n,t),o=e.parentNode(r);return Wa(e,o,i,e.nextSibling(r),!1),i}var cE=uE,lE=()=>!1;function dE(t,n,e){return lE(t,n,e)}function uE(t,n,e,i){if(t[$n])return;let r;e.type&8?r=bt(i):r=aE(n,e),t[$n]=r}var Lu=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Vu=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Tf(n,e).matches!==null&&this.queries[e].setDirty()}},Ya=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=gE(n):this.predicate=n}},Bu=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},ju=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,fE(e,o)),this.matchTNodeWithReadOption(n,e,Ba(e,n,o,!1,!1))}else i===nt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Ba(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===O||r===lt||r===nt&&e.type&4)this.addMatch(e.index,-2);else{let o=Ba(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function fE(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function mE(t,n){return t.type&11?Or(t,n):t.type&4?lc(t,n):null}function hE(t,n,e,i){return e===-1?mE(n,t):e===-2?pE(t,n,i):ko(t,t[M],e,n)}function pE(t,n,e){if(e===O)return Or(n,t);if(e===nt)return lc(n,t);if(e===lt)return l_(n,t)}function d_(t,n,e,i){let r=n[tn].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(hE(n,d,s[c+1],e.metadata.read))}}r.matches=a}return r.matches}function Hu(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=d_(t,n,r,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let u=we;u<d.length;u++){let m=d[u];m[zn]===m[Oe]&&Hu(m[M],m,l,i)}if(d[Ii]!==null){let u=d[Ii];for(let m=0;m<u.length;m++){let h=u[m];Hu(h[M],h,l,i)}}}}}return i}function Sf(t,n){return t[tn].queries[n].queryList}function u_(t,n,e){let i=new wn((e&4)===4);return Vp(t,n,i,i.destroy),(n[tn]??=new Vu).queries.push(new Lu(i))-1}function f_(t,n,e){let i=he();return i.firstCreatePass&&(h_(i,new Ya(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),u_(i,T(),n)}function m_(t,n,e,i){let r=he();if(r.firstCreatePass){let o=Le();h_(r,new Ya(n,e,i),o.index),bE(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return u_(r,T(),e)}function gE(t){return t.split(",").map(n=>n.trim())}function h_(t,n,e){t.queries===null&&(t.queries=new Bu),t.queries.track(new ju(n,e))}function bE(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Tf(t,n){return t.queries.getByIndex(n)}function p_(t,n){let e=t[M],i=Tf(e,n);return i.crossesNgTemplate?Hu(e,t,n,[]):d_(e,t,i,n)}function g_(t,n,e){let i,r=Xr(()=>{i._dirtyCounter();let o=_E(i,t);if(n&&o===void 0)throw new k(-951,!1);return o});return i=r[Ce],i._dirtyCounter=G(0),i._flatValue=void 0,r}function Af(t){return g_(!0,!1,t)}function Rf(t){return g_(!0,!0,t)}function b_(t,n){let e=t[Ce];e._lView=T(),e._queryIndex=n,e._queryList=Sf(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function _E(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[A]&4)return n?void 0:Ge;let r=Sf(e,i),o=p_(e,i);return r.reset(o,nb),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var Zn=class{};var To=class extends Zn{injector;componentFactoryResolver=new Fu(this);instance=null;constructor(n){super();let e=new yi([...n.providers,{provide:Zn,useValue:this},{provide:uc,useValue:this.componentFactoryResolver}],n.parent||br(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function __(t,n,e=null){return new To({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var vE=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Cd(!1,e.type),r=i.length>0?__([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=v({token:t,providedIn:"environment",factory:()=>new t(R(Ee))})}return t})();function j(t){return Oo(()=>{let n=v_(t),e=B(y({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===ef.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(vE).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Ht.Emulated,styles:t.styles||Ge,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&kn("NgStandalone"),y_(e);let i=t.dependencies;return e.directiveDefs=Og(i,yE),e.pipeDefs=Og(i,_p),e.id=DE(e),e})}function yE(t){return jn(t)||bd(t)}function Ie(t){return Oo(()=>({type:t.type,bootstrap:t.bootstrap||Ge,declarations:t.declarations||Ge,imports:t.imports||Ge,exports:t.exports||Ge,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function xE(t,n){if(t==null)return Hn;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=ic.None,c=null),e[o]=[i,a,c],n[o]=s}return e}function wE(t){if(t==null)return Hn;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function N(t){return Oo(()=>{let n=v_(t);return y_(n),n})}function v_(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Hn,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||Ge,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:xE(t.inputs,n),outputs:wE(t.outputs),debugInfo:null}}function y_(t){t.features?.forEach(n=>n(t))}function Og(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function DE(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function CE(t){return Object.getPrototypeOf(t.prototype).constructor}function Me(t){let n=CE(t.type),e=!0,i=[t];for(;n;){let r;if(nn(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new k(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let s=t;s.inputs=cu(t.inputs),s.declaredInputs=cu(t.declaredInputs),s.outputs=cu(t.outputs);let a=r.hostBindings;a&&SE(t,a);let c=r.viewQuery,l=r.contentQueries;if(c&&IE(t,c),l&&ME(t,l),EE(t,r),bp(t.outputs,r.outputs),nn(r)&&r.data.animation){let d=t.data;d.animation=(d.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(t),a===Me&&(e=!1)}}n=Object.getPrototypeOf(n)}kE(i)}function EE(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function kE(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Mr(r.hostAttrs,e=Mr(e,r.hostAttrs))}}function cu(t){return t===Hn?{}:t===Ge?[]:t}function IE(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function ME(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function SE(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function x_(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=Mr(t.mergedAttrs,t.attrs);let d=t.tView=ff(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),Ti(t,!1);let c=AE(e,n,t,i);Aa()&&vf(e,n,c,t),Sr(c,n);let l=Qb(c,n,c,t);n[i+be]=l,hf(n,l),dE(l,t,n)}function TE(t,n,e,i,r,o,s,a,c,l,d){let u=e+be,m;return n.firstCreatePass?(m=ji(n,u,4,s||null,a||null),ka()&&i_(n,t,m,vt(n.consts,l),xf),$g(n,m)):m=n.data[u],x_(m,t,n,e,i,r,o,c),xr(m)&&cc(n,t,m),l!=null&&Lo(t,m,d),m}function Ao(t,n,e,i,r,o,s,a,c,l,d){let u=e+be,m;if(n.firstCreatePass){if(m=ji(n,u,4,s||null,a||null),l!=null){let h=vt(n.consts,l);m.localNames=[];for(let p=0;p<h.length;p+=2)m.localNames.push(h[p],-1)}}else m=n.data[u];return x_(m,t,n,e,i,r,o,c),l!=null&&Lo(t,m,d),m}function je(t,n,e,i,r,o,s,a){let c=T(),l=he(),d=vt(l.consts,o);return TE(c,l,t,n,e,i,r,d,void 0,s,a),je}var AE=RE;function RE(t,n,e,i){return yo(!0),n[ue].createComment("")}function Yn(t){return typeof t=="function"&&t[Ce]!==void 0}function Of(t){return Yn(t)&&typeof t.set=="function"}var Nf=new g("");function Nr(t){return!!t&&typeof t.then=="function"}function Ff(t){return!!t&&typeof t.subscribe=="function"}var w_=new g("");var Pf=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=f(w_,{optional:!0})??[];injector=f(Z);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=_r(this.injector,r);if(Nr(o))e.push(o);else if(Ff(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Lf=new g("");function D_(){Ol(()=>{let t="";throw new k(600,t)})}function C_(t){return t.isBoundToModule}var OE=10;var yt=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=f(xn);afterRenderManager=f(oc);zonelessEnabled=f(wo);rootEffectScheduler=f(Ra);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new D;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=f(Ai);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ge(e=>!e))}constructor(){f(Ut,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=f(Ee);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=Z.NULL){return this._injector.get(S).run(()=>{ce(oe.BootstrapComponentStart);let s=e instanceof dc;if(!this._injector.get(Pf).done){let p="";throw new k(405,p)}let c;s?c=e:c=this._injector.get(uc).resolveComponentFactory(e),this.componentTypes.push(c.componentType);let l=C_(c)?void 0:this._injector.get(Zn),d=i||c.selector,u=c.create(r,[],d,l),m=u.location.nativeElement,h=u.injector.get(Nf,null);return h?.registerApplication(m),u.onDestroy(()=>{this.detachView(u.hostView),Eo(this.components,u),h?.unregisterApplication(m)}),this._loadComponent(u),ce(oe.BootstrapComponentEnd,u),u})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ce(oe.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(rc.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ce(oe.ChangeDetectionEnd),new k(101,!1);let e=I(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,I(e),this.afterTick.next(),ce(oe.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ve,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<OE;){ce(oe.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ce(oe.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!bo(r))continue;let o=i&&!this.zonelessEnabled?0:1;Gb(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>bo(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Eo(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Lf,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Eo(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new k(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Eo(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function ae(t,n,e,i){let r=T(),o=Wn();if(ln(r,o,n)){let s=he(),a=vo();lC(a,r,t,n,e,i)}return ae}var zu=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function lu(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function NE(t,n,e,i){let r,o,s=0,a=t.length-1,c=void 0;if(Array.isArray(n)){I(i);let l=n.length-1;for(I(null);s<=a&&s<=l;){let d=t.at(s),u=n[s],m=lu(s,d,s,u,e);if(m!==0){m<0&&t.updateValue(s,u),s++;continue}let h=t.at(a),p=n[l],b=lu(a,h,l,p,e);if(b!==0){b<0&&t.updateValue(a,p),a--,l--;continue}let _=e(s,d),E=e(a,h),le=e(s,u);if(Object.is(le,E)){let Ke=e(l,p);Object.is(Ke,_)?(t.swap(s,a),t.updateValue(a,p),l--,a--):t.move(a,s),t.updateValue(s,u),s++;continue}if(r??=new Qa,o??=Fg(t,s,a,e),Uu(t,r,s,le))t.updateValue(s,u),s++,a++;else if(o.has(le))r.set(_,t.detach(s)),a--;else{let Ke=t.create(s,n[s]);t.attach(s,Ke),s++,a++}}for(;s<=l;)Ng(t,r,e,s,n[s]),s++}else if(n!=null){I(i);let l=n[Symbol.iterator]();I(null);let d=l.next();for(;!d.done&&s<=a;){let u=t.at(s),m=d.value,h=lu(s,u,s,m,e);if(h!==0)h<0&&t.updateValue(s,m),s++,d=l.next();else{r??=new Qa,o??=Fg(t,s,a,e);let p=e(s,m);if(Uu(t,r,s,p))t.updateValue(s,m),s++,a++,d=l.next();else if(!o.has(p))t.attach(s,t.create(s,m)),s++,a++,d=l.next();else{let b=e(s,u);r.set(b,t.detach(s)),a--}}}for(;!d.done;)Ng(t,r,e,t.length,d.value),d=l.next()}for(;s<=a;)t.destroy(t.detach(a--));r?.forEach(l=>{t.destroy(l)})}function Uu(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function Ng(t,n,e,i,r){if(Uu(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function Fg(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var Qa=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function ee(t,n,e,i,r,o,s,a){kn("NgControlFlow");let c=T(),l=he(),d=vt(l.consts,o);return Ao(c,l,t,n,e,i,r,d,256,s,a),Vf}function Vf(t,n,e,i,r,o,s,a){kn("NgControlFlow");let c=T(),l=he(),d=vt(l.consts,o);return Ao(c,l,t,n,e,i,r,d,512,s,a),Vf}function te(t,n){kn("NgControlFlow");let e=T(),i=Wn(),r=e[i]!==ct?e[i]:-1,o=r!==-1?Ka(e,be+r):void 0,s=0;if(ln(e,i,t)){let a=I(null);try{if(o!==void 0&&Xb(o,s),t!==-1){let c=be+t,l=Ka(e,c),d=qu(e[M],c),u=e_(l,d,e),m=Vo(e,d,n,{dehydratedView:u});Bo(l,m,s,Tr(d,u))}}finally{I(a)}}else if(o!==void 0){let a=Kb(o,s);a!==void 0&&(a[ke]=n)}}var $u=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-we}};function fc(t,n){return n}var Wu=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function Hi(t,n,e,i,r,o,s,a,c,l,d,u,m){kn("NgControlFlow");let h=T(),p=he(),b=c!==void 0,_=T(),E=a?s.bind(_[Je][ke]):s,le=new Wu(b,E);_[be+t]=le,Ao(h,p,t+1,n,e,i,r,vt(p.consts,o),256),b&&Ao(h,p,t+2,c,l,d,u,vt(p.consts,m),512)}var Gu=class extends zu{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-we}at(n){return this.getLView(n)[ke].$implicit}attach(n,e){let i=e[Di];this.needsIndexUpdate||=n!==this.length,Bo(this.lContainer,e,n,Tr(this.templateTNode,i)),FE(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,PE(this.lContainer,n),LE(this.lContainer,n)}create(n,e){let i=Ga(this.lContainer,this.templateTNode.tView.ssrId);return Vo(this.hostLView,this.templateTNode,new $u(this.lContainer,e,n),{dehydratedView:i})}destroy(n){sc(n[M],n)}updateValue(n,e){this.getLView(n)[ke].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[ke].$index=n}getLView(n){return VE(this.lContainer,n)}};function zi(t){let n=I(null),e=rn();try{let i=T(),r=i[M],o=i[e],s=e+1,a=Ka(i,s);if(o.liveCollection===void 0){let l=qu(r,s);o.liveCollection=new Gu(a,i,l)}else o.liveCollection.reset();let c=o.liveCollection;if(NE(c,t,o.trackByFn,n),c.updateIndexes(),o.hasEmptyBlock){let l=Wn(),d=c.length===0;if(ln(i,l,d)){let u=e+2,m=Ka(i,u);if(d){let h=qu(r,u),p=e_(m,h,i),b=Vo(i,h,void 0,{dehydratedView:p});Bo(m,b,0,Tr(h,p))}else r.firstUpdatePass&&OC(m),Xb(m,0)}}}finally{I(n)}}function Ka(t,n){return t[n]}function FE(t,n){if(t.length<=we)return;let e=we+n,i=t[e],r=i?i[Un]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[bn];HD(o,r),Ni.delete(i[_n]),r.detachedLeaveAnimationFns=void 0}}function PE(t,n){if(t.length<=we)return;let e=we+n,i=t[e],r=i?i[Un]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function LE(t,n){return So(t,n)}function VE(t,n){return Kb(t,n)}function qu(t,n){return wa(t,n)}function q(t,n,e){let i=T(),r=Wn();if(ln(i,r,n)){let o=he(),s=vo();Bb(s,i,t,n,i[ue],e)}return q}function Zu(t,n,e,i,r){Cf(n,t,e,r?"class":"style",i)}function w(t,n,e,i){let r=T(),o=r[M],s=t+be,a=o.firstCreatePass?If(s,r,2,n,xf,ka(),e,i):o.data[s];if(yn(a)){let c=r[Ot].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(a_(l),()=>(Pg(t,n,r,a,i),w))}}return Pg(t,n,r,a,i),w}function Pg(t,n,e,i,r){if(wf(i,e,t,n,E_),xr(i)){let o=e[M];cc(o,e,i),rf(o,i,e)}r!=null&&Lo(e,i)}function C(){let t=he(),n=Le(),e=Df(n);return t.firstCreatePass&&Mf(t,e),Hd(e)&&zd(),Bd(),e.classesWithoutHost!=null&&Cw(e)&&Zu(t,e,T(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&Ew(e)&&Zu(t,e,T(),e.stylesWithoutHost,!1),C}function X(t,n,e,i){return w(t,n,e,i),C(),X}function dt(t,n,e,i){let r=T(),o=r[M],s=t+be,a=o.firstCreatePass?qC(s,o,2,n,e,i):o.data[s];return wf(a,r,t,n,E_),i!=null&&Lo(r,a),dt}function xt(){let t=Le(),n=Df(t);return Hd(n)&&zd(),Bd(),xt}function wt(t,n,e,i){return dt(t,n,e,i),xt(),wt}var E_=(t,n,e,i,r)=>(yo(!0),xb(n[ue],i,Kp()));function Bf(t,n,e){let i=T(),r=i[M],o=t+be,s=r.firstCreatePass?If(o,i,8,"ng-container",xf,ka(),n,e):r.data[o];if(wf(s,i,t,"ng-container",BE),xr(s)){let a=i[M];cc(a,i,s),rf(a,s,i)}return e!=null&&Lo(i,s),Bf}function jf(){let t=he(),n=Le(),e=Df(n);return t.firstCreatePass&&Mf(t,e),jf}function In(t,n,e){return Bf(t,n,e),jf(),In}var BE=(t,n,e,i,r)=>(yo(!0),_D(n[ue],""));function Mn(){return T()}function Qn(t,n,e){let i=T(),r=Wn();if(ln(i,r,n)){let o=he(),s=vo();jb(s,i,t,n,i[ue],e)}return Qn}var jo="en-US";var jE=jo;function k_(t){typeof t=="string"&&(jE=t.toLowerCase().replace(/_/g,"-"))}function fe(t,n,e){let i=T(),r=he(),o=Le();return I_(r,i,i[ue],o,t,n,e),fe}function I_(t,n,e,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=su(i,n,o),YC(i,t,n,s,e,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let u=0;u<d.length;u+=2){let m=d[u],h=d[u+1];c??=su(i,n,o),Tg(i,n,m,h,r,c)}if(l&&l.length)for(let u of l)c??=su(i,n,o),Tg(i,n,u,r,r,c)}}function U(t=1){return Qp(t)}function HE(t,n){let e=null,i=kD(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?Ib(t,o,!0):SD(i,o))return r}return e}function Se(t){let n=T()[Je][Xe];if(!n.projection){let e=t?t.length:1,i=n.projection=Cp(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?HE(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function Q(t,n=0,e,i,r,o){let s=T(),a=he(),c=i?t+1:null;c!==null&&Ao(s,a,c,i,r,o,null,e);let l=ji(a,be+t,16,null,e||null);l.projection===null&&(l.projection=n),Wd();let u=!s[Di]||jd();s[Je][Xe].projection[l.projection]===null&&c!==null?zE(s,a,c):u&&!ec(l)&&XD(a,s,l)}function zE(t,n,e){let i=be+e,r=n.data[i],o=t[i],s=Ga(o,r.tView.ssrId),a=Vo(t,r,void 0,{dehydratedView:s});Bo(o,a,0,Tr(r,s))}function $t(t,n,e,i){return m_(t,n,e,i),$t}function He(t,n,e){return f_(t,n,e),He}function $(t){let n=T(),e=he(),i=Ma();_o(i+1);let r=Tf(e,i);if(t.dirty&&Np(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=p_(n,i);t.reset(o,nb),t.notifyOnChanges()}return!0}return!1}function W(){return Sf(T(),Ma())}function mc(t,n,e,i,r){return b_(n,m_(t,e,i,r)),mc}function hc(t,n,e,i){return b_(t,f_(n,e,i)),hc}function pc(t=1){_o(Ma()+t)}function ut(t){let n=Gd();return Od(n,be+t)}function Pa(t,n){return t<<17|n<<2}function Fi(t){return t>>17&32767}function UE(t){return(t&2)==2}function $E(t,n){return t&131071|n<<17}function Yu(t){return t|2}function Rr(t){return(t&131068)>>2}function du(t,n){return t&-131069|n<<2}function WE(t){return(t&1)===1}function Qu(t){return t|1}function GE(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=Fi(s),c=Rr(s);t[i]=e;let l=!1,d;if(Array.isArray(e)){let u=e;d=u[1],(d===null||pr(u,d)>0)&&(l=!0)}else d=e;if(r)if(c!==0){let m=Fi(t[a+1]);t[i+1]=Pa(m,a),m!==0&&(t[m+1]=du(t[m+1],i)),t[a+1]=$E(t[a+1],i)}else t[i+1]=Pa(a,0),a!==0&&(t[a+1]=du(t[a+1],i)),a=i;else t[i+1]=Pa(c,0),a===0?a=i:t[c+1]=du(t[c+1],i),c=i;l&&(t[i+1]=Yu(t[i+1])),Lg(t,d,i,!0),Lg(t,d,i,!1),qE(n,d,t,i,o),s=Pa(a,c),o?n.classBindings=s:n.styleBindings=s}function qE(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&pr(o,n)>=0&&(e[i+1]=Qu(e[i+1]))}function Lg(t,n,e,i){let r=t[e+1],o=n===null,s=i?Fi(r):Rr(r),a=!1;for(;s!==0&&(a===!1||o);){let c=t[s],l=t[s+1];ZE(c,n)&&(a=!0,t[s+1]=i?Qu(l):Yu(l)),s=i?Fi(l):Rr(l)}a&&(t[e+1]=i?Yu(r):Qu(r))}function ZE(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?pr(t,n)>=0:!1}var jt={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function YE(t){return t.substring(jt.key,jt.keyEnd)}function QE(t){return KE(t),M_(t,S_(t,0,jt.textEnd))}function M_(t,n){let e=jt.textEnd;return e===n?-1:(n=jt.keyEnd=XE(t,jt.key=n,e),S_(t,n,e))}function KE(t){jt.key=0,jt.keyEnd=0,jt.value=0,jt.valueEnd=0,jt.textEnd=t.length}function S_(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function XE(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Sn(t,n,e){return T_(t,n,e,!1),Sn}function V(t,n){return T_(t,n,null,!0),V}function Wt(t){ek(sk,JE,t,!0)}function JE(t,n){for(let e=QE(n);e>=0;e=M_(n,e))va(t,YE(n),!0)}function T_(t,n,e,i){let r=T(),o=he(),s=Yd(2);if(o.firstUpdatePass&&R_(o,t,s,i),n!==ct&&ln(r,s,n)){let a=o.data[rn()];O_(o,a,r,r[ue],t,r[s+1]=ck(n,e),i,s)}}function ek(t,n,e,i){let r=he(),o=Yd(2);r.firstUpdatePass&&R_(r,null,o,i);let s=T();if(e!==ct&&ln(s,o,e)){let a=r.data[rn()];if(N_(a,i)&&!A_(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=ha(c,e||"")),Zu(r,a,s,e,i)}else ak(r,a,s,s[ue],s[o+1],s[o+1]=ok(t,n,e),i,o)}}function A_(t,n){return n>=t.expandoStartIndex}function R_(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[rn()],s=A_(t,e);N_(o,i)&&n===null&&!s&&(n=!1),n=tk(r,o,n,i),GE(r,o,n,e,s,i)}}function tk(t,n,e,i){let r=Gp(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=uu(null,t,n,e,i),e=Ro(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=uu(r,t,n,e,i),o===null){let c=nk(t,n,i);c!==void 0&&Array.isArray(c)&&(c=uu(null,t,n,c[1],i),c=Ro(c,n.attrs,i),ik(t,n,i,c))}else o=rk(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function nk(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Rr(i)!==0)return t[Fi(i)]}function ik(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Fi(r)]=i}function rk(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=Ro(i,s,e)}return Ro(i,n.attrs,e)}function uu(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=Ro(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function Ro(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),va(t,s,e?!0:n[++o]))}return t===void 0?null:t}function ok(t,n,e){if(e==null||e==="")return Ge;let i=[],r=zt(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function sk(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&va(t,i,e)}function ak(t,n,e,i,r,o,s,a){r===ct&&(r=Ge);let c=0,l=0,d=0<r.length?r[0]:null,u=0<o.length?o[0]:null;for(;d!==null||u!==null;){let m=c<r.length?r[c+1]:void 0,h=l<o.length?o[l+1]:void 0,p=null,b;d===u?(c+=2,l+=2,m!==h&&(p=u,b=h)):u===null||d!==null&&d<u?(c+=2,p=d):(l+=2,p=u,b=h),p!==null&&O_(t,n,e,i,p,b,s,a),d=c<r.length?r[c]:null,u=l<o.length?o[l]:null}}function O_(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let c=t.data,l=c[a+1],d=WE(l)?Vg(c,n,e,r,Rr(l),s):void 0;if(!Xa(d)){Xa(o)||UE(l)&&(o=Vg(c,null,e,r,a,s));let u=Rd(rn(),e);eC(i,s,u,r,o)}}function Vg(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let c=t[r],l=Array.isArray(c),d=l?c[1]:c,u=d===null,m=e[r+1];m===ct&&(m=u?Ge:void 0);let h=u?ya(m,i):d===i?m:void 0;if(l&&!Xa(h)&&(h=ya(c,i)),Xa(h)&&(a=h,s))return a;let p=t[r+1];r=s?Fi(p):Rr(p)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=ya(c,i))}return a}function Xa(t){return t!==void 0}function ck(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=ma(zt(t)))),t}function N_(t,n){return(t.flags&(n?8:16))!==0}function Ze(t,n=""){let e=T(),i=he(),r=t+be,o=i.firstCreatePass?ji(i,r,1,n,null):i.data[r],s=lk(i,e,o,n);e[r]=s,Aa()&&vf(i,e,s,o),Ti(o,!1)}var lk=(t,n,e,i)=>(yo(!0),gD(n[ue],i));function dk(t,n,e,i=""){return ln(t,Wn(),e)?n+mo(e)+i:ct}function Ui(t){return Dt("",t),Ui}function Dt(t,n,e){let i=T(),r=dk(i,t,n,e);return r!==ct&&uk(i,rn(),r),Dt}function uk(t,n,e){let i=Rd(n,t);bD(t[ue],i,e)}function gc(t,n,e){Of(n)&&(n=n());let i=T(),r=Wn();if(ln(i,r,n)){let o=he(),s=vo();Bb(s,i,t,n,i[ue],e)}return gc}function Hf(t,n){let e=Of(t);return e&&t.set(n),e}function bc(t,n){let e=T(),i=he(),r=Le();return I_(i,e,e[ue],r,t,n),bc}var F_={};function _c(t){kn("NgLet");let n=he(),e=T(),i=t+be,r=ji(n,i,128,null,null);return Ti(r,!1),Da(n,e,i,F_),_c}function zf(t){let n=he(),e=T(),i=rn();return Da(n,e,i,t),t}function Uf(t){let n=Gd(),e=Od(n,be+t);if(e===F_)throw new k(314,!1);return e}function Bg(t,n,e){let i=he();i.firstCreatePass&&P_(n,i.data,i.blueprint,nn(t),e)}function P_(t,n,e,i,r){if(t=Ue(t),Array.isArray(t))for(let o=0;o<t.length;o++)P_(t[o],n,e,i,r);else{let o=he(),s=T(),a=Le(),c=vi(t)?t:Ue(t.provide),l=kd(t),d=a.providerIndexes&1048575,u=a.directiveStart,m=a.providerIndexes>>20;if(vi(t)||!t.multi){let h=new Oi(l,r,pe,null),p=mu(c,n,r?d:d+m,u);p===-1?(pu($a(a,s),o,c),fu(o,t,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(h),s.push(h)):(e[p]=h,s[p]=h)}else{let h=mu(c,n,d+m,u),p=mu(c,n,d,d+m),b=h>=0&&e[h],_=p>=0&&e[p];if(r&&!_||!r&&!b){pu($a(a,s),o,c);let E=hk(r?mk:fk,e.length,r,i,l,t);!r&&_&&(e[p].providerFactory=E),fu(o,t,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(E),s.push(E)}else{let E=L_(e[r?p:h],l,!r&&i);fu(o,t,h>-1?h:p,E)}!r&&i&&_&&e[p].componentProviders++}}}function fu(t,n,e,i){let r=vi(n),o=Tp(n);if(r||o){let c=(o?Ue(n.useClass):n).prototype.ngOnDestroy;if(c){let l=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let d=l.indexOf(e);d===-1?l.push(e,[i,c]):l[d+1].push(i,c)}else l.push(e,c)}}}function L_(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function mu(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function fk(t,n,e,i,r){return Ku(this.multi,[])}function mk(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=ko(i,i[M],this.providerFactory.index,r);s=c.slice(0,a),Ku(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],Ku(o,s);return s}function Ku(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function hk(t,n,e,i,r,o){let s=new Oi(t,e,pe,null);return s.multi=[],s.index=n,s.componentProviders=0,L_(s,r,i&&!e),s}function Te(t,n){return e=>{e.providersResolver=(i,r)=>Bg(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>Bg(i,r?r(n):n,!0))}}function $f(t,n){let e=Zd()+t,i=T();return i[e]===ct?o_(i,e,n()):ZC(i,e)}function vc(t,n,e){return gk(T(),Zd(),t,n,e)}function pk(t,n){let e=t[n];return e===ct?void 0:e}function gk(t,n,e,i,r,o){let s=n+e;return ln(t,s,r)?o_(t,s+1,o?i.call(o,r):i(r)):pk(t,s+1)}function $i(t,n){return lc(t,n)}var V_=(()=>{class t{applicationErrorHandler=f(xn);appRef=f(yt);taskService=f(Ai);ngZone=f(S);zonelessEnabled=f(wo);tracing=f(Ut,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ie;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(uo):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(f(nu,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?ng:Xd;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(uo+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function B_(){return[{provide:en,useExisting:V_},{provide:S,useClass:fo},{provide:wo,useValue:!0}]}function bk(){return typeof $localize<"u"&&$localize.locale||jo}var yc=new g("",{factory:()=>f(yc,{optional:!0,skipSelf:!0})||bk()});function De(t){return fp(t)}function _e(t,n){return Xr(t,n?.equal)}var _k=t=>t;function Ct(t,n){if(typeof t=="function"){let e=Jl(t,_k,n?.equal);return j_(e,n?.debugName)}else{let e=Jl(t.source,t.computation,t.equal);return j_(e,t.debugName)}}function j_(t,n){let e=t[Ce],i=t;return i.set=r=>dp(e,r),i.update=r=>up(e,r),i.asReadonly=xo.bind(t),i}function Zf(t){let n=t.request,e=t.params??n??(()=>null);return new Fr(e,yk(t),t.defaultValue,t.equal?vk(t.equal):void 0,t.debugName,t.injector??f(Z))}var Wf=class{value;isLoading;constructor(n,e){this.value=n,this.value.set=this.set.bind(this),this.value.update=this.update.bind(this),this.value.asReadonly=xo,this.isLoading=_e(()=>this.status()==="loading"||this.status()==="reloading",void 0)}isError=_e(()=>this.status()==="error");update(n){this.set(n(De(this.value)))}isValueDefined=_e(()=>this.isError()?!1:this.value()!==void 0);_snapshot;get snapshot(){return this._snapshot??=_e(()=>{let n=this.status();return n==="error"?{status:"error",error:this.error()}:{status:n,value:this.value()}})}hasValue(){return this.isValueDefined()}asReadonly(){return this}},Fr=class extends Wf{loaderFn;equal;debugName;pendingTasks;state;extRequest;effectRef;pendingController;resolvePendingTask=void 0;destroyed=!1;unregisterOnDestroy;status;error;constructor(n,e,i,r,o,s,a){super(_e(()=>{let c=this.state().stream?.();if(!c||this.state().status==="loading"&&this.error())return i;if(!Gf(c))throw new xc(this.error());return c.value},{equal:r}),o),this.loaderFn=e,this.equal=r,this.debugName=o,this.extRequest=Ct({source:n,computation:c=>({request:c,reload:0})}),this.state=Ct({source:this.extRequest,computation:(c,l)=>{if(l){let d=c.request===void 0?"idle":"loading";return{extRequest:c,status:d,previousStatus:H_(l.value),stream:l.value.extRequest.request===c.request?l.value.stream:void 0}}else{let d=a?.(c.request);a=void 0;let u=c.request===void 0?"idle":d?"resolved":"loading";return{extRequest:c,status:u,previousStatus:"idle",stream:d}}}}),this.effectRef=tt(this.loadEffect.bind(this),{injector:s,manualCleanup:!0}),this.pendingTasks=s.get(Er),this.unregisterOnDestroy=s.get(st).onDestroy(()=>this.destroy()),this.status=_e(()=>H_(this.state()),void 0),this.error=_e(()=>{let c=this.state().stream?.();return c&&!Gf(c)?c.error:void 0},void 0)}set(n){if(this.destroyed)return;let e=De(this.error),i=De(this.state);if(!e){let r=De(this.value);if(i.status==="local"&&(this.equal?this.equal(r,n):r===n))return}this.state.set({extRequest:i.extRequest,status:"local",previousStatus:"local",stream:G({value:n},void 0)}),this.abortInProgressLoad()}reload(){let{status:n}=De(this.state);return n==="idle"||n==="loading"?!1:(this.extRequest.update(({request:e,reload:i})=>({request:e,reload:i+1})),!0)}destroy(){this.destroyed=!0,this.unregisterOnDestroy(),this.effectRef.destroy(),this.abortInProgressLoad(),this.state.set({extRequest:{request:void 0,reload:0},status:"idle",previousStatus:"idle",stream:void 0})}async loadEffect(){let n=this.extRequest(),{status:e,previousStatus:i}=De(this.state);if(n.request===void 0)return;if(e!=="loading")return;this.abortInProgressLoad();let r=this.resolvePendingTask=this.pendingTasks.add(),{signal:o}=this.pendingController=new AbortController;try{let s=await De(()=>this.loaderFn({params:n.request,abortSignal:o,previous:{status:i}}));if(o.aborted||De(this.extRequest)!==n)return;this.state.set({extRequest:n,status:"resolved",previousStatus:"resolved",stream:s})}catch(s){if(o.aborted||De(this.extRequest)!==n)return;this.state.set({extRequest:n,status:"resolved",previousStatus:"error",stream:G({error:Ho(s)},void 0)})}finally{r?.(),r=void 0}}abortInProgressLoad(){De(()=>this.pendingController?.abort()),this.pendingController=void 0,this.resolvePendingTask?.(),this.resolvePendingTask=void 0}};function vk(t){return(n,e)=>n===void 0||e===void 0?n===e:t(n,e)}function yk(t){return xk(t)?t.stream:async n=>{try{return G({value:await t.loader(n)},void 0)}catch(e){return G({error:Ho(e)},void 0)}}}function xk(t){return!!t.stream}function H_(t){switch(t.status){case"loading":return t.extRequest.reload===0?"loading":"reloading";case"resolved":return Gf(t.stream())?"resolved":"error";default:return t.status}}function Gf(t){return t.error===void 0}function Ho(t){return wk(t)?t:new qf(t)}function wk(t){return t instanceof Error||typeof t=="object"&&typeof t.name=="string"&&typeof t.message=="string"}var xc=class extends Error{constructor(n){super(n.message,{cause:n})}},qf=class extends Error{constructor(n){super(String(n),{cause:n})}};var q_=Symbol("InputSignalNode#UNSET"),Bk=B(y({},Jr),{transformFn:void 0,applyValueToInputSignal(t,n){ai(t,n)}});function Z_(t,n){let e=Object.create(Bk);e.value=t,e.transformFn=n?.transform;function i(){if(On(e),e.value===q_){let r=null;throw new k(-950,r)}return e.value}return i[Ce]=e,i}var Kn=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>Ju(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function z_(t,n){return Z_(t,n)}function jk(t){return Z_(q_,t)}var Y_=(z_.required=jk,z_);function U_(t,n){return Af(n)}function Hk(t,n){return Rf(n)}var Uo=(U_.required=Hk,U_);function $_(t,n){return Af(n)}function zk(t,n){return Rf(n)}var Q_=($_.required=zk,$_);var Qf=new g(""),Uk=new g("");function zo(t){return!t.moduleRef}function $k(t){let n=zo(t)?t.r3Injector:t.moduleRef.injector,e=n.get(S);return e.run(()=>{zo(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(xn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),zo(t)){let o=()=>n.destroy(),s=t.platformInjector.get(Qf);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(Qf);s.add(o),t.moduleRef.onDestroy(()=>{Eo(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return Gk(i,e,()=>{let o=n.get(Ai),s=o.add(),a=n.get(Pf);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(yc,jo);if(k_(c||jo),!n.get(Uk,!0))return zo(t)?n.get(yt):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(zo(t)){let d=n.get(yt);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return Wk?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var Wk;function Gk(t,n,e){try{let i=e();return Nr(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var wc=null;function qk(t=[],n){return Z.create({name:n,providers:[{provide:po,useValue:"platform"},{provide:Qf,useValue:new Set([()=>wc=null])},...t]})}function Zk(t=[]){if(wc)return wc;let n=qk(t);return wc=n,D_(),Yk(n),n}function Yk(t){let n=t.get(Ja,null);_r(t,()=>{n?.forEach(e=>e())})}var Qk=1e4;var _U=Qk-1e3;var ze=(()=>{class t{static __NG_ELEMENT_ID__=Kk}return t})();function Kk(t){return Xk(Le(),T(),(t&16)===16)}function Xk(t,n,e){if(yn(t)&&!e){let i=_t(t.index,n);return new qn(i,i)}else if(t.type&175){let i=n[Je];return new qn(i,n)}return null}function K_(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;ce(oe.BootstrapApplicationStart);try{let o=r?.injector??Zk(i),s=[B_(),rg,...e||[]],a=new To({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return $k({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{ce(oe.BootstrapApplicationEnd)}}function H(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function ft(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Yf=Symbol("NOT_SET"),X_=new Set,Jk=B(y({},Jr),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Yf,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Yf&&!or(this))return this.signal;try{for(let r of this.cleanup??X_)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=mn(this),i;try{i=this.userFn.apply(null,n)}finally{Nn(this,e)}return(this.value===Yf||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Kf=class extends Io{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(st),s),this.scheduler=r;for(let a of gf){let c=e[a];if(c===void 0)continue;let l=Object.create(Jk);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(On(l),l.value),l.signal[Ce]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??X_)e()}finally{Fn(n)}}};function J_(t,n){let e=n?.injector??f(Z),i=e.get(en),r=e.get(oc),o=e.get(Ut,null,{optional:!0});r.impl??=e.get(bf);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(Cr,null,{optional:!0}),c=new Kf(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function Dc(t,n){let e=jn(t),i=n.elementInjector||br();return new Ar(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var ev=null;function Et(){return ev}function Xf(t){ev??=t}var $o=class{},Cc=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:()=>f(tv),providedIn:"platform"})}return t})();var tv=(()=>{class t extends Cc{_location;_history;_doc=f(P);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Et().getBaseHref(this._doc)}onPopState(e){let i=Et().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Et().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function rv(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function nv(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Xn(t){return t&&t[0]!=="?"?`?${t}`:t}var Ec=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:()=>f(tI),providedIn:"root"})}return t})(),eI=new g(""),tI=(()=>{class t extends Ec{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??f(P).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return rv(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Xn(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+Xn(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+Xn(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(R(Cc),R(eI,8))};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var kc=(()=>{class t{_subject=new D;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=rI(nv(iv(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Xn(i))}normalize(e){return t.stripTrailingSlash(iI(this._basePath,iv(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Xn(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Xn(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Xn;static joinWithSlash=rv;static stripTrailingSlash=nv;static \u0275fac=function(i){return new(i||t)(R(Ec))};static \u0275prov=v({token:t,factory:()=>nI(),providedIn:"root"})}return t})();function nI(){return new kc(R(Ec))}function iI(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function iv(t){return t.replace(/\/index.html$/,"")}function rI(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Wi=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=f(Z);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(pe(lt))};static \u0275dir=N({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Be]})}return t})();function Ic(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var Gi=class{};var Jf="browser";function ov(t){return t===Jf}var Wo=class{_doc;constructor(n){this._doc=n}manager},Mc=(()=>{class t extends Wo{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(R(P))};static \u0275prov=v({token:t,factory:t.\u0275fac})}return t})(),Ac=new g(""),im=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof Mc));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof Mc);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new k(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(R(Ac),R(S))};static \u0275prov=v({token:t,factory:t.\u0275fac})}return t})(),em="ng-app-id";function sv(t){for(let n of t)n.remove()}function av(t,n){let e=n.createElement("style");return e.textContent=t,e}function cI(t,n,e,i){let r=t.head?.querySelectorAll(`style[${em}="${n}"],link[${em}="${n}"]`);if(r)for(let o of r)o.removeAttribute(em),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function nm(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var rm=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,cI(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,av);i?.forEach(r=>this.addUsage(r,this.external,nm))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(sv(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])sv(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,av(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,nm(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(R(P),R(Pi),R(Vi,8),R(Li))};static \u0275prov=v({token:t,factory:t.\u0275fac})}return t})(),tm={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},om=/%COMP%/g;var lv="%COMP%",lI=`_nghost-${lv}`,dI=`_ngcontent-${lv}`,uI=!0,fI=new g("",{factory:()=>uI});function mI(t){return dI.replace(om,t)}function hI(t){return lI.replace(om,t)}function dv(t,n){return n.map(e=>e.replace(om,t))}var sm=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Go(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Tc?r.applyToHost(e):r instanceof qo&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(i.encapsulation){case Ht.Emulated:o=new Tc(c,l,i,this.appId,d,s,a,u);break;case Ht.ShadowDom:return new Sc(c,e,i,s,a,this.nonce,u,l);case Ht.ExperimentalIsolatedShadowDom:return new Sc(c,e,i,s,a,this.nonce,u);default:o=new qo(c,l,i,d,s,a,u);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(R(im),R(rm),R(Pi),R(fI),R(P),R(S),R(Vi),R(Ut,8))};static \u0275prov=v({token:t,factory:t.\u0275fac})}return t})(),Go=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(tm[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(cv(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(cv(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new k(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=tm[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=tm[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(sn.DashCase|sn.Important)?n.style.setProperty(e,i,r&sn.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&sn.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=Et().getGlobalEventTarget(this.doc,n),!n))throw new k(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function cv(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Sc=class extends Go{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=dv(i.id,l);for(let u of l){let m=document.createElement("style");s&&m.setAttribute("nonce",s),m.textContent=u,this.shadowRoot.appendChild(m)}let d=i.getExternalStyles?.();if(d)for(let u of d){let m=nm(u,r);s&&m.setAttribute("nonce",s),this.shadowRoot.appendChild(m)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},qo=class extends Go{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?dv(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Ni.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Tc=class extends qo{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,e,i,o,s,a,c,l),this.contentAttr=mI(l),this.hostAttr=hI(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Rc=class t extends $o{supportsDOMEvents=!0;static makeCurrent(){Xf(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=pI();return e==null?null:gI(e)}resetBaseElement(){Zo=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Ic(document.cookie,n)}},Zo=null;function pI(){return Zo=Zo||document.head.querySelector("base"),Zo?Zo.getAttribute("href"):null}function gI(t){return new URL(t,document.baseURI).pathname}var bI=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac})}return t})(),uv=["alt","control","meta","shift"],_I={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},vI={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},fv=(()=>{class t extends Wo{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Et().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),uv.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(e,i){let r=_I[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),uv.forEach(s=>{if(s!==r){let a=vI[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(R(P))};static \u0275prov=v({token:t,factory:t.\u0275fac})}return t})();async function am(t,n,e){let i=y({rootComponent:t},yI(n,e));return K_(i)}function yI(t,n){return{platformRef:n?.platformRef,appProviders:[...EI,...t?.providers??[]],platformProviders:CI}}function xI(){Rc.makeCurrent()}function wI(){return new At}function DI(){return tf(document),document}var CI=[{provide:Li,useValue:Jf},{provide:Ja,useValue:xI,multi:!0},{provide:P,useFactory:DI}];var EI=[{provide:po,useValue:"root"},{provide:At,useFactory:wI},{provide:Ac,useClass:Mc,multi:!0},{provide:Ac,useClass:fv,multi:!0},sm,rm,im,{provide:Ve,useExisting:sm},{provide:Gi,useClass:bI},[]];var qt=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var lm=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},dm=class{encodeKey(n){return mv(n)}encodeValue(n){return mv(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function kI(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var II=/%(\d[a-f0-9])/gi,MI={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function mv(t){return encodeURIComponent(t).replace(II,(n,e)=>MI[e]??n)}function Oc(t){return`${t}`}var Gt=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new dm,n.fromString){if(n.fromObject)throw new k(2805,!1);this.map=kI(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Oc):[Oc(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Oc(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Oc(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function SI(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function hv(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function pv(t){return typeof Blob<"u"&&t instanceof Blob}function gv(t){return typeof FormData<"u"&&t instanceof FormData}function TI(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var bv="Content-Type",_v="Accept",vv="text/plain",yv="application/json",AI=`${yv}, ${vv}, */*`,qi=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(SI(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new k(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new qt,this.context??=new lm,!this.params)this.params=new Gt,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e.indexOf("?"),c=a===-1?"?":a<e.length-1?"&":"";this.urlWithParams=e+c+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||hv(this.body)||pv(this.body)||gv(this.body)||TI(this.body)?this.body:this.body instanceof Gt?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||gv(this.body)?null:pv(this.body)?this.body.type||null:hv(this.body)?null:typeof this.body=="string"?vv:this.body instanceof Gt?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?yv:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,u=n.referrer||this.referrer,m=n.integrity||this.integrity,h=n.referrerPolicy||this.referrerPolicy,p=n.transferCache??this.transferCache,b=n.timeout??this.timeout,_=n.body!==void 0?n.body:this.body,E=n.withCredentials??this.withCredentials,le=n.reportProgress??this.reportProgress,Ke=n.headers||this.headers,Re=n.params||this.params,fn=n.context??this.context;return n.setHeaders!==void 0&&(Ke=Object.keys(n.setHeaders).reduce((Mt,ii)=>Mt.set(ii,n.setHeaders[ii]),Ke)),n.setParams&&(Re=Object.keys(n.setParams).reduce((Mt,ii)=>Mt.set(ii,n.setParams[ii]),Re)),new t(e,i,_,{params:Re,headers:Ke,context:fn,reportProgress:le,responseType:r,withCredentials:E,transferCache:p,keepalive:o,cache:a,priority:s,timeout:b,mode:c,redirect:l,credentials:d,referrer:u,integrity:m,referrerPolicy:h})}},Tn=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(Tn||{}),Yo=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new qt,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},um=class t extends Yo{constructor(n={}){super(n)}type=Tn.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Pr=class t extends Yo{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Tn.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Zi=class extends Yo{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},RI=200,OI=204;var NI=/^\)\]\}',?\n/;var FI=(()=>{class t{xhrFactory;tracingService=f(Ut,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new k(-2800,!1);let i=this.xhrFactory;return Xt(null).pipe(pi(()=>new K(o=>{let s=i.build();if(s.open(e.method,e.urlWithParams),e.withCredentials&&(s.withCredentials=!0),e.headers.forEach((_,E)=>s.setRequestHeader(_,E.join(","))),e.headers.has(_v)||s.setRequestHeader(_v,AI),!e.headers.has(bv)){let _=e.detectContentTypeHeader();_!==null&&s.setRequestHeader(bv,_)}if(e.timeout&&(s.timeout=e.timeout),e.responseType){let _=e.responseType.toLowerCase();s.responseType=_!=="json"?_:"text"}let a=e.serializeBody(),c=null,l=()=>{if(c!==null)return c;let _=s.statusText||"OK",E=new qt(s.getAllResponseHeaders()),le=s.responseURL||e.url;return c=new um({headers:E,status:s.status,statusText:_,url:le}),c},d=this.maybePropagateTrace(()=>{let{headers:_,status:E,statusText:le,url:Ke}=l(),Re=null;E!==OI&&(Re=typeof s.response>"u"?s.responseText:s.response),E===0&&(E=Re?RI:0);let fn=E>=200&&E<300;if(e.responseType==="json"&&typeof Re=="string"){let Mt=Re;Re=Re.replace(NI,"");try{Re=Re!==""?JSON.parse(Re):null}catch(ii){Re=Mt,fn&&(fn=!1,Re={error:ii,text:Re})}}fn?(o.next(new Pr({body:Re,headers:_,status:E,statusText:le,url:Ke||void 0})),o.complete()):o.error(new Zi({error:Re,headers:_,status:E,statusText:le,url:Ke||void 0}))}),u=this.maybePropagateTrace(_=>{let{url:E}=l(),le=new Zi({error:_,status:s.status||0,statusText:s.statusText||"Unknown Error",url:E||void 0});o.error(le)}),m=u;e.timeout&&(m=this.maybePropagateTrace(_=>{let{url:E}=l(),le=new Zi({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:E||void 0});o.error(le)}));let h=!1,p=this.maybePropagateTrace(_=>{h||(o.next(l()),h=!0);let E={type:Tn.DownloadProgress,loaded:_.loaded};_.lengthComputable&&(E.total=_.total),e.responseType==="text"&&s.responseText&&(E.partialText=s.responseText),o.next(E)}),b=this.maybePropagateTrace(_=>{let E={type:Tn.UploadProgress,loaded:_.loaded};_.lengthComputable&&(E.total=_.total),o.next(E)});return s.addEventListener("load",d),s.addEventListener("error",u),s.addEventListener("timeout",m),s.addEventListener("abort",u),e.reportProgress&&(s.addEventListener("progress",p),a!==null&&s.upload&&s.upload.addEventListener("progress",b)),s.send(a),o.next({type:Tn.Sent}),()=>{s.removeEventListener("error",u),s.removeEventListener("abort",u),s.removeEventListener("load",d),s.removeEventListener("timeout",m),e.reportProgress&&(s.removeEventListener("progress",p),a!==null&&s.upload&&s.upload.removeEventListener("progress",b)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(i){return new(i||t)(R(Gi))};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function PI(t,n){return n(t)}function LI(t,n,e){return(i,r)=>_r(e,()=>n(i,o=>t(o,r)))}var VI=new g("",{factory:()=>[]}),xv=new g(""),BI=new g("",{factory:()=>!0});var jI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=R(FI),r},providedIn:"root"})}return t})();var HI=(()=>{class t{backend;injector;chain=null;pendingTasks=f(Er);contributeToStability=f(BI);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(VI),...this.injector.get(xv,[])]));this.chain=i.reduceRight((r,o)=>LI(r,o,this.injector),PI)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Zl(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(R(jI),R(Ee))};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),zI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=R(HI),r},providedIn:"root"})}return t})();function cm(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var wv=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof qi)o=e;else{let c;r.headers instanceof qt?c=r.headers:c=new qt(r.headers);let l;r.params&&(r.params instanceof Gt?l=r.params:l=new Gt({fromObject:r.params})),o=new qi(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=Xt(o).pipe(ql(c=>this.handler.handle(c)));if(e instanceof qi||r.observe==="events")return s;let a=s.pipe(ye(c=>c instanceof Pr));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ge(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new k(2806,!1);return c.body}));case"blob":return a.pipe(ge(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new k(2807,!1);return c.body}));case"text":return a.pipe(ge(c=>{if(c.body!==null&&typeof c.body!="string")throw new k(2808,!1);return c.body}));default:return a.pipe(ge(c=>c.body))}case"response":return a;default:throw new k(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Gt().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,cm(r,i))}post(e,i,r={}){return this.request("POST",e,cm(r,i))}put(e,i,r={}){return this.request("PUT",e,cm(r,i))}static \u0275fac=function(i){return new(i||t)(R(zI))};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var UI=new g(""),$I="b",WI="h",GI="s",qI="st",ZI="u",YI="rt",QI=new g(""),KI=["GET","HEAD"];function XI(t,n){let s=n,{isCacheActive:e}=s,i=Ih(s,["isCacheActive"]),{transferCache:r,method:o}=t;return!(!e||r===!1||o==="POST"&&!i.includePostRequests&&!r||o!=="POST"&&!KI.includes(o)||!i.includeRequestsWithAuthHeaders&&tM(t)||i.filter?.(t)===!1)}function JI(t,n){let{includeHeaders:e}=t,i=e;return typeof n=="object"&&n.includeHeaders&&(i=n.includeHeaders),i}function eM(t,n,e,i){let{transferCache:r}=t;if(!XI(t,n))return null;if(i)throw new k(2803,!1);let o=t.url,s=nM(t,o),a=e.get(s,null),c=JI(n,r);if(a){let{[$I]:l,[YI]:d,[WI]:u,[GI]:m,[qI]:h,[ZI]:p}=a,b=l;switch(d){case"arraybuffer":b=Cv(l);break;case"blob":b=new Blob([Cv(l)]);break}let _=new qt(u);return new Pr({body:b,headers:_,status:m,statusText:h,url:p})}return null}function tM(t){return t.headers.has("authorization")||t.headers.has("proxy-authorization")}function Dv(t){return[...t.keys()].sort().map(n=>`${n}=${t.getAll(n)}`).join("&")}function nM(t,n){let{params:e,method:i,responseType:r}=t,o=Dv(e),s=t.serializeBody();s instanceof URLSearchParams?s=Dv(s):typeof s!="string"&&(s="");let a=[i,r,n,s,o].join("|"),c=iM(a);return c}function iM(t){let n=0;for(let e of t)n=Math.imul(31,n)+e.charCodeAt(0)<<0;return n+=2147483648,n.toString()}function Cv(t){let n=atob(t);return Uint8Array.from(n,i=>i.charCodeAt(0)).buffer}var Ev=(()=>{let t=Nc("json");return t.arrayBuffer=Nc("arraybuffer"),t.blob=Nc("blob"),t.text=Nc("text"),t})();function Nc(t){return function(e,i){let r=i?.injector??f(Z),o=r.get(QI,null,{optional:!0}),s=r.get(nf,null,{optional:!0}),a=r.get(UI,null,{optional:!0}),c=l=>{if(o&&s&&l){let d=eM(l,o,s,a);if(d)try{let u=d.body,m=i?.parse?i.parse(u):u;return G({value:m})}catch{}}};return new fm(r,()=>rM(e,t),i?.defaultValue,i?.debugName,i?.parse,i?.equal,c)}}function rM(t,n){let e=typeof t=="function"?t():t;if(e===void 0)return;typeof e=="string"&&(e={url:e});let i=e.headers instanceof qt?e.headers:new qt(e.headers),r=e.params instanceof Gt?e.params:new Gt({fromObject:e.params});return new qi(e.method??"GET",e.url,e.body??null,{headers:i,params:r,reportProgress:e.reportProgress,withCredentials:e.withCredentials,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,responseType:n,context:e.context,transferCache:e.transferCache,credentials:e.credentials,referrer:e.referrer,referrerPolicy:e.referrerPolicy,integrity:e.integrity,timeout:e.timeout})}var fm=class extends Fr{client;_headers=Ct({source:this.extRequest,computation:()=>{}});_progress=Ct({source:this.extRequest,computation:()=>{}});_statusCode=Ct({source:this.extRequest,computation:()=>{}});headers=_e(()=>this.status()==="resolved"||this.status()==="error"?this._headers():void 0);progress=this._progress.asReadonly();statusCode=this._statusCode.asReadonly();constructor(n,e,i,r,o,s,a){super(e,({params:c,abortSignal:l})=>{let d,u=()=>d.unsubscribe();l.addEventListener("abort",u);let m=G({value:void 0}),h,p=new Promise(_=>h=_),b=_=>{m.set(_),h?.(m),h=void 0};return d=this.client.request(c).subscribe({next:_=>{switch(_.type){case Tn.Response:this._headers.set(_.headers),this._statusCode.set(_.status);try{b({value:o?o(_.body):_.body})}catch(E){b({error:Ho(E)})}break;case Tn.DownloadProgress:this._progress.set(_);break}},error:_=>{_ instanceof Zi&&(this._headers.set(_.headers),this._statusCode.set(_.status)),b({error:_}),l.removeEventListener("abort",u)},complete:()=>{h&&b({error:new k(991,!1)}),l.removeEventListener("abort",u)}}),p},i,s,r,n,a),this.client=n.get(wv)}set(n){super.set(n),this._headers.set(void 0),this._progress.set(void 0),this._statusCode.set(void 0)}};var mm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=R(oM),r},providedIn:"root"})}return t})(),oM=(()=>{class t extends mm{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case at.NONE:return i;case at.HTML:return Cn(i,"HTML")?zt(i):nc(this._doc,String(i)).toString();case at.STYLE:return Cn(i,"Style")?zt(i):i;case at.SCRIPT:if(Cn(i,"Script"))return zt(i);throw new k(5200,!1);case at.URL:return Cn(i,"URL")?zt(i):Fo(String(i));case at.RESOURCE_URL:if(Cn(i,"ResourceURL"))return zt(i);throw new k(5201,!1);default:throw new k(5202,!1)}}bypassSecurityTrustHtml(e){return of(e)}bypassSecurityTrustStyle(e){return sf(e)}bypassSecurityTrustScript(e){return af(e)}bypassSecurityTrustUrl(e){return cf(e)}bypassSecurityTrustResourceUrl(e){return lf(e)}static \u0275fac=function(i){return new(i||t)(R(P))};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Qo(t){return t.buttons===0||t.detail===0}function Ko(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var hm;function kv(){if(hm==null){let t=typeof document<"u"?document.head:null;hm=!!(t&&(t.createShadowRoot||t.attachShadow))}return hm}function pm(t){if(kv()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function $e(t){return t.composedPath?t.composedPath()[0]:t.target}var gm;try{gm=typeof Intl<"u"&&Intl.v8BreakIterator}catch{gm=!1}var ve=(()=>{class t{_platformId=f(Li);isBrowser=this._platformId?ov(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||gm)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Xo;function Iv(){if(Xo==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Xo=!0}))}finally{Xo=Xo||!1}return Xo}function Lr(t){return Iv()?t:!!t.capture}function dn(t){return t instanceof O?t.nativeElement:t}var Mv=new g("cdk-input-modality-detector-options"),Sv={ignoreKeys:[18,17,224,91,16]},Tv=650,bm={passive:!0,capture:!0},Av=(()=>{class t{_platform=f(ve);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new ui(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=$e(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<Tv||(this._modality.next(Qo(e)?"keyboard":"mouse"),this._mostRecentTarget=$e(e))};_onTouchstart=e=>{if(Ko(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=$e(e)};constructor(){let e=f(S),i=f(P),r=f(Mv,{optional:!0});if(this._options=y(y({},Sv),r),this.modalityDetected=this._modality.pipe(oo(1)),this.modalityChanged=this.modalityDetected.pipe(na()),this._platform.isBrowser){let o=f(Ve).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,bm),o.listen(i,"mousedown",this._onMousedown,bm),o.listen(i,"touchstart",this._onTouchstart,bm)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Jo=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Jo||{}),Rv=new g("cdk-focus-monitor-default-options"),Fc=Lr({passive:!0,capture:!0}),es=(()=>{class t{_ngZone=f(S);_platform=f(ve);_inputModalityDetector=f(Av);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=f(P);_stopInputModalityDetector=new D;constructor(){let e=f(Rv,{optional:!0});this._detectionMode=e?.detectionMode||Jo.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=$e(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=dn(e);if(!this._platform.isBrowser||r.nodeType!==1)return Xt();let o=pm(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new D,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=dn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=dn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Jo.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Jo.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?Tv:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=$e(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Fc),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Fc)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Pe(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Fc),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Fc),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Pc=new WeakMap,it=(()=>{class t{_appRef;_injector=f(Z);_environmentInjector=f(Ee);load(e){let i=this._appRef=this._appRef||this._injector.get(yt),r=Pc.get(i);r||(r={loaders:new Set,refs:[]},Pc.set(i,r),i.onDestroy(()=>{Pc.get(i)?.refs.forEach(o=>o.destroy()),Pc.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Dc(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _m=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
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
`],encapsulation:2,changeDetection:0})}return t})(),Lc;function sM(){if(Lc===void 0&&(Lc=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(Lc=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return Lc}function aM(t){return sM()?.createHTML(t)||t}function Ov(t,n,e){let i=e.sanitize(at.HTML,n);t.innerHTML=aM(i||"")}function Vr(t){return Array.isArray(t)?t:[t]}var Nv=new Set,Yi,Vc=(()=>{class t{_platform=f(ve);_nonce=f(Vi,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):lM}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&cM(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function cM(t,n){if(!Nv.has(t))try{Yi||(Yi=document.createElement("style"),n&&Yi.setAttribute("nonce",n),Yi.setAttribute("type","text/css"),document.head.appendChild(Yi)),Yi.sheet&&(Yi.sheet.insertRule(`@media ${t} {body{ }}`,0),Nv.add(t))}catch(e){console.error(e)}}function lM(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var vm=(()=>{class t{_mediaMatcher=f(Vc);_zone=f(S);_queries=new Map;_destroySubject=new D;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return Fv(Vr(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=Fv(Vr(e)).map(s=>this._registerQuery(s).observable),o=$l(r);return o=fr(o.pipe(hi(1)),o.pipe(oo(1),ro(0))),o.pipe(ge(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:c,query:l})=>{a.matches=a.matches||c,a.breakpoints[l]=c}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new K(s=>{let a=c=>this._zone.run(()=>s.next(c));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(ht(i),ge(({matches:s})=>({query:e,matches:s})),Pe(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Fv(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var Pv=new g("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),Lv=new g("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),dM=0,ts=(()=>{class t{_ngZone=f(S);_defaultOptions=f(Lv,{optional:!0});_liveElement;_document=f(P);_sanitizer=f(mm);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=f(Pv,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:Ov(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${dM++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var uM=200,Bc=class{_letterKeyStream=new D;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new D;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:uM;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(Kl(e=>this._pressedLetters.push(e)),ro(n),ye(()=>this._pressedLetters.length>0),ge(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function kt(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Br=class{_items;_activeItemIndex=G(-1);_activeItem=G(null);_wrap=!1;_typeaheadSubscription=ie.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof wn?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Yn(n)&&(this._effectRef=tt(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new D;change=new D;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Bc(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||kt(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Yn(this._items)?this._items():this._items instanceof wn?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var ns=class extends Br{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var is=class extends Br{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var wm={},Ae=class t{_appId=f(Pi);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),wm.hasOwnProperty(n)||(wm[n]=0),`${n}${e?t._infix+"-":""}${wm[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})};var Hv=" ";function zv(t,n,e){let i=Uv(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(Hv)))}function Dm(t,n,e){let i=Uv(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(Hv)):t.removeAttribute(n)}function Uv(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var fM=new g("cdk-dir-doc",{providedIn:"root",factory:()=>f(P)}),mM=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function $v(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?mM.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Jn=(()=>{class t{get value(){return this.valueSignal()}valueSignal=G("ltr");change=new F;constructor(){let e=f(fM,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set($v(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Ne(t){return t==null?"":typeof t=="string"?t:`${t}px`}function rs(t){return t!=null&&`${t}`!="false"}function Cm(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Em=class{_box;_destroyed=new D;_resizeSubject=new D;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new K(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(ye(e=>e.some(i=>i.target===n)),ra({bufferSize:1,refCount:!0}),Pe(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},Wv=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=f(S);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Em(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Gv={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var pM=new g("MATERIAL_ANIMATIONS"),qv=null;function km(){return f(pM,{optional:!0})?.animationsDisabled||f(No,{optional:!0})==="NoopAnimations"?"di-disabled":(qv??=f(Vc).matchMedia("(prefers-reduced-motion)").matches,qv?"reduced-motion":"enabled")}function Fe(){return km()!=="enabled"}var gM=["notch"],bM=["matFormFieldNotchedOutline",""],_M=["*"],Zv=["iconPrefixContainer"],Yv=["textPrefixContainer"],Qv=["iconSuffixContainer"],Kv=["textSuffixContainer"],vM=["textField"],yM=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],xM=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function wM(t,n){t&1&&X(0,"span",21)}function DM(t,n){if(t&1&&(w(0,"label",20),Q(1,1),ee(2,wM,1,0,"span",21),C()),t&2){let e=U(2);q("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),ae("for",e._control.disableAutomaticLabeling?null:e._control.id),x(2),te(!e.hideRequiredMarker&&e._control.required?2:-1)}}function CM(t,n){if(t&1&&ee(0,DM,3,5,"label",20),t&2){let e=U();te(e._hasFloatingLabel()?0:-1)}}function EM(t,n){t&1&&X(0,"div",7)}function kM(t,n){}function IM(t,n){if(t&1&&je(0,kM,0,0,"ng-template",13),t&2){U(2);let e=ut(1);q("ngTemplateOutlet",e)}}function MM(t,n){if(t&1&&(w(0,"div",9),ee(1,IM,1,1,null,13),C()),t&2){let e=U();q("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),x(),te(e._forceDisplayInfixLabel()?-1:1)}}function SM(t,n){t&1&&(w(0,"div",10,2),Q(2,2),C())}function TM(t,n){t&1&&(w(0,"div",11,3),Q(2,3),C())}function AM(t,n){}function RM(t,n){if(t&1&&je(0,AM,0,0,"ng-template",13),t&2){U();let e=ut(1);q("ngTemplateOutlet",e)}}function OM(t,n){t&1&&(w(0,"div",14,4),Q(2,4),C())}function NM(t,n){t&1&&(w(0,"div",15,5),Q(2,5),C())}function FM(t,n){t&1&&X(0,"div",16)}function PM(t,n){t&1&&(w(0,"div",18),Q(1,6),C())}function LM(t,n){if(t&1&&(w(0,"mat-hint",22),Ze(1),C()),t&2){let e=U(2);q("id",e._hintLabelId),x(),Ui(e.hintLabel)}}function VM(t,n){if(t&1&&(w(0,"div",19),ee(1,LM,2,2,"mat-hint",22),Q(2,7),X(3,"div",23),Q(4,8),C()),t&2){let e=U();x(),te(e.hintLabel?1:-1)}}var Im=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-label"]]})}return t})(),ry=new g("MatError");var Mm=(()=>{class t{align="start";id=f(Ae).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Qn("id",r.id),ae("align",null),V("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),oy=new g("MatPrefix");var sy=new g("MatSuffix");var ay=new g("FloatingLabelParent"),Xv=(()=>{class t{_elementRef=f(O);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=f(Wv);_ngZone=f(S);_parent=f(ay);_resizeSubscription=new ie;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return BM(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&V("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function BM(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var Jv="mdc-line-ripple--active",jc="mdc-line-ripple--deactivating",ey=(()=>{class t{_elementRef=f(O);_cleanupTransitionEnd;constructor(){let e=f(S),i=f(qe);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(jc),e.add(Jv)}deactivate(){this._elementRef.nativeElement.classList.add(jc)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(jc);e.propertyName==="opacity"&&r&&i.remove(Jv,jc)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),ty=(()=>{class t{_elementRef=f(O);_ngZone=f(S);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&He(gM,5),i&2){let o;$(o=W())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&V("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:bM,ngContentSelectors:_M,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Se(),wt(0,"div",1),dt(1,"div",2,0),Q(3),xt(),wt(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),Hc=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t})}return t})();var zc=new g("MatFormField"),Uc=new g("MAT_FORM_FIELD_DEFAULT_OPTIONS"),ny="fill",jM="auto",iy="fixed",HM="translateY(-50%)",Sm=(()=>{class t{_elementRef=f(O);_changeDetectorRef=f(ze);_platform=f(ve);_idGenerator=f(Ae);_ngZone=f(S);_defaults=f(Uc,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Uo("iconPrefixContainer");_textPrefixContainerSignal=Uo("textPrefixContainer");_iconSuffixContainerSignal=Uo("iconSuffixContainer");_textSuffixContainerSignal=Uo("textSuffixContainer");_prefixSuffixContainers=_e(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Q_(Im);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=rs(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||jM}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||ny;this._appearanceSignal.set(i)}_appearanceSignal=G(ny);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||iy}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||iy}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new D;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Fe();constructor(){let e=this._defaults,i=f(Jn);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),tt(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=_e(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(ht([void 0,void 0]),ge(()=>[i.errorState,i.userAriaDescribedBy]),ia(),ye(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Pe(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),pn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){J_({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=_e(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${s+a}px`,h=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,p=`var(--mat-mdc-form-field-label-transform, ${HM} translateX(${h}))`,b=s+a+c+l;return[p,b]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(mc(o,r._labelChild,Im,5),$t(o,Hc,5)(o,oy,5)(o,sy,5)(o,ry,5)(o,Mm,5)),i&2){pc();let s;$(s=W())&&(r._formFieldControl=s.first),$(s=W())&&(r._prefixChildren=s),$(s=W())&&(r._suffixChildren=s),$(s=W())&&(r._errorChildren=s),$(s=W())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(hc(r._iconPrefixContainerSignal,Zv,5)(r._textPrefixContainerSignal,Yv,5)(r._iconSuffixContainerSignal,Qv,5)(r._textSuffixContainerSignal,Kv,5),He(vM,5)(Zv,5)(Yv,5)(Qv,5)(Kv,5)(Xv,5)(ty,5)(ey,5)),i&2){pc(4);let o;$(o=W())&&(r._textField=o.first),$(o=W())&&(r._iconPrefixContainer=o.first),$(o=W())&&(r._textPrefixContainer=o.first),$(o=W())&&(r._iconSuffixContainer=o.first),$(o=W())&&(r._textSuffixContainer=o.first),$(o=W())&&(r._floatingLabel=o.first),$(o=W())&&(r._notchedOutline=o.first),$(o=W())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&V("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Te([{provide:zc,useExisting:t},{provide:ay,useExisting:t}])],ngContentSelectors:xM,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Se(yM),je(0,CM,1,1,"ng-template",null,0,$i),w(2,"div",6,1),fe("click",function(s){return r._control.onContainerClick(s)}),ee(4,EM,1,0,"div",7),w(5,"div",8),ee(6,MM,2,2,"div",9),ee(7,SM,3,0,"div",10),ee(8,TM,3,0,"div",11),w(9,"div",12),ee(10,RM,1,1,null,13),Q(11),C(),ee(12,OM,3,0,"div",14),ee(13,NM,3,0,"div",15),C(),ee(14,FM,1,0,"div",16),C(),w(15,"div",17),ee(16,PM,2,0,"div",18)(17,VM,5,1,"div",19),C()),i&2){let o;x(2),V("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),x(2),te(!r._hasOutline()&&!r._control.disabled?4:-1),x(2),te(r._hasOutline()?6:-1),x(),te(r._hasIconPrefix?7:-1),x(),te(r._hasTextPrefix?8:-1),x(2),te(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),x(2),te(r._hasTextSuffix?12:-1),x(),te(r._hasIconSuffix?13:-1),x(),te(r._hasOutline()?-1:14),x(),V("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();x(),te((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[Xv,ty,Wi,ey,Mm],styles:[`.mdc-text-field {
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
`],encapsulation:2,changeDetection:0})}return t})();var zM=20,cy=(()=>{class t{_ngZone=f(S);_platform=f(ve);_renderer=f(Ve).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new D;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=zM){return this._platform.isBrowser?new K(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(ta(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Xt()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(ye(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=dn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var UM=20,os=(()=>{class t{_platform=f(ve);_listeners;_viewportSize=null;_change=new D;_document=f(P);constructor(){let e=f(S),i=f(Ve).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=UM){return e>0?this._change.pipe(ta(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ss=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},jr=class extends ss{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},An=class extends ss{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Tm=class extends ss{element;constructor(n){super(),this.element=n instanceof O?n.nativeElement:n}},Hr=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof jr)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof An)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Tm)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},$c=class extends Hr{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Zn,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||Z.NULL,o=r.get(Ee,i.injector);e=Dc(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var as=(()=>{class t extends Hr{_moduleRef=f(Zn,{optional:!0});_document=f(P);_viewContainerRef=f(lt);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new F;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[Me]})}return t})();var Wc=class{enable(){}disable(){}attach(){}};function Am(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function ly(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function Kc(t,n){return new Gc(t.get(cy),t.get(os),t.get(S),n)}var Gc=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Am(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}};var Ki=class{positionStrategy;scrollStrategy=new Wc;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var qc=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var by=(()=>{class t{_attachedOverlays=[];_document=f(P);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),_y=(()=>{class t extends by{_ngZone=f(S);_renderer=f(Ve).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=an(t)))(r||t)}})();static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),vy=(()=>{class t extends by{_platform=f(ve);_ngZone=f(S);_renderer=f(Ve).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=$e(e)};_clickListener=e=>{let i=$e(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,c))){if(dy(a.overlayElement,i)||dy(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=an(t)))(r||t)}})();static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function dy(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var yy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
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
`],encapsulation:2,changeDetection:0})}return t})(),xy=(()=>{class t{_platform=f(ve);_containerElement;_document=f(P);_styleLoader=f(it);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Cm()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Cm()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(yy)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Rm=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Om(t){return t&&t.nodeType===1}var Zc=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new D;_attachments=new D;_detachments=new D;_positionStrategy;_scrollStrategy;_locationChanges=ie.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new D;_outsidePointerEvents=new D;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,c,l,d=!1,u,m){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=u,this._renderer=m,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=cn(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=y(y({},this._config),n),this._updateElementSize()}setDirection(n){this._config=B(y({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Ne(this._config.width),n.height=Ne(this._config.height),n.minWidth=Ne(this._config.minWidth),n.minHeight=Ne(this._config.minHeight),n.maxWidth=Ne(this._config.maxWidth),n.maxHeight=Ne(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Om(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Rm(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Vr(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=cn(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},uy="cdk-overlay-connected-position-bounding-box",$M=/([A-Za-z%]+)$/;function wy(t,n){return new Yc(n,t.get(os),t.get(P),t.get(ve),t.get(xy))}var Yc=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new D;_resizeSubscription=ie.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(uy),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,e,a),d=this._getOverlayFit(l,e,i,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(d,l,i)){o.push({position:a,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:l,originPoint:c,position:a,overlayRect:e})}if(o.length){let a=null,c=-1;for(let l of o){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&Qi(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(uy),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof O?this._origin.nativeElement:Om(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=my(e),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let d=0-s,u=s+o.width-i.width,m=0-a,h=a+o.height-i.height,p=this._subtractOverflows(o.width,d,u),b=this._subtractOverflows(o.height,m,h),_=p*b;return{visibleArea:_,isCompletelyWithinViewport:o.width*o.height===_,fitsInViewportVertically:b===o.height,fitsInViewportHorizontally:p==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=fy(this._overlayRef.getConfig().minHeight),a=fy(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=my(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),d=0,u=0;return r.width<=o.width?d=l||-s:d=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?u=c||-a:u=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:d,y:u},{x:n.x+d,y:n.y+u}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!WM(this._lastScrollVisibility,i)){let r=new qc(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let h=Math.min(i.bottom-n.y+i.top,n.y),p=this._lastBoundingBoxSize.height;o=h*2,s=n.y-h,o>p&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-p/2)}let c=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,l=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,d,u,m;if(l)m=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(c)u=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let h=Math.min(i.right-n.x+i.left,n.x),p=this._lastBoundingBoxSize.width;d=h*2,u=n.x-h,d>p&&!this._isInitialRender&&!this._growAfterOpen&&(u=n.x-p/2)}return{top:s,left:u,bottom:a,right:m,width:d,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=Ne(i.width),r.height=Ne(i.height),r.top=Ne(i.top)||"auto",r.bottom=Ne(i.bottom)||"auto",r.left=Ne(i.left)||"auto",r.right=Ne(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Ne(o)),s&&(r.maxWidth=Ne(s))}this._lastBoundingBoxSize=i,Qi(this._boundingBox.style,r)}_resetBoundingBoxStyles(){Qi(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){Qi(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();Qi(i,this._getExactOverlayY(e,n,d)),Qi(i,this._getExactOverlayX(e,n,d))}else i.position="static";let a="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=Ne(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=Ne(s.maxWidth):o&&(i.maxWidth="")),Qi(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=Ne(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=Ne(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:ly(n,i),isOriginOutsideView:Am(n,i),isOverlayClipped:ly(e,i),isOverlayOutsideView:Am(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Vr(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof O)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function Qi(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function fy(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split($M);return!e||e==="px"?parseFloat(n):null}return t||null}function my(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function WM(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var hy="cdk-global-overlay-wrapper";function Nm(t){return new Qc}var Qc=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(hy),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,u=this._xOffset,m=this._overlayRef.getConfig().direction==="rtl",h="",p="",b="";c?b="flex-start":d==="center"?(b="center",m?p=u:h=u):m?d==="left"||d==="end"?(b="flex-end",h=u):(d==="right"||d==="start")&&(b="flex-start",p=u):d==="left"||d==="start"?(b="flex-start",h=u):(d==="right"||d==="end")&&(b="flex-end",p=u),n.position=this._cssPosition,n.marginLeft=c?"0":h,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":p,e.justifyContent=b,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(hy),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}};var cs=new g("OVERLAY_DEFAULT_CONFIG");function Xc(t,n){t.get(it).load(yy);let e=t.get(xy),i=t.get(P),r=t.get(Ae),o=t.get(yt),s=t.get(Jn),a=t.get(qe,null,{optional:!0})||t.get(Ve).createRenderer(null,null),c=new Ki(n),l=t.get(cs,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in i.body?c.usePopover=n?.usePopover??l:c.usePopover=!1;let d=i.createElement("div"),u=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),u.appendChild(d),c.usePopover&&(u.setAttribute("popover","manual"),u.classList.add("cdk-overlay-popover"));let m=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return Om(m)?m.after(u):m?.type==="parent"?m.element.appendChild(u):e.getContainerElement().appendChild(u),new Zc(new $c(d,o,t),u,d,c,t.get(S),t.get(_y),i,t.get(kc),t.get(vy),n?.disableAnimations??t.get(No,null,{optional:!0})==="NoopAnimations",t.get(Ee),a)}var GM=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],qM=new g("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=f(Z);return()=>Kc(t)}}),zr=(()=>{class t{elementRef=f(O);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),Dy=new g("cdk-connected-overlay-default-config"),Jc=(()=>{class t{_dir=f(Jn,{optional:!0});_injector=f(Z);_overlayRef;_templatePortal;_backdropSubscription=ie.EMPTY;_attachSubscription=ie.EMPTY;_detachSubscription=ie.EMPTY;_positionSubscription=ie.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=f(qM);_ngZone=f(S);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new F;positionChange=new F;attach=new F;detach=new F;overlayKeydown=new F;overlayOutsideClick=new F;constructor(){let e=f(nt),i=f(lt),r=f(Dy,{optional:!0}),o=f(cs,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new An(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=GM);let e=this._overlayRef=Xc(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!kt(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=$e(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Ki({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=wy(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof zr?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof zr?this.origin.elementRef.nativeElement:this.origin instanceof O?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Ql(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",H],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",H],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",H],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",H],push:[2,"cdkConnectedOverlayPush","push",H],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",H],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",H],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Be]})}return t})();var It=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(It||{}),Fm=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=It.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},Ey=Lr({passive:!0,capture:!0}),Pm=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,Ey)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,Ey)))}_delegateEventHandler=n=>{let e=$e(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},ls={enterDuration:225,exitDuration:150},ZM=800,ky=Lr({passive:!0,capture:!0}),Iy=["mousedown","touchstart"],My=["mouseup","mouseleave","touchend","touchcancel"],YM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
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
`],encapsulation:2,changeDetection:0})}return t})(),ds=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Pm;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=dn(i)),o&&o.get(it).load(YM)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=y(y({},ls),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||QM(n,e,r),a=n-r.left,c=e-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),m=u.transitionProperty,h=u.transitionDuration,p=m==="none"||h==="0s"||h==="0s, 0s"||r.width===0&&r.height===0,b=new Fm(this,d,i,p);d.style.transform="scale3d(1, 1, 1)",b.state=It.FADING_IN,i.persistent||(this._mostRecentTransientRipple=b);let _=null;return!p&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let E=()=>{_&&(_.fallbackTimer=null),clearTimeout(Ke),this._finishRippleTransition(b)},le=()=>this._destroyRipple(b),Ke=setTimeout(le,l+100);d.addEventListener("transitionend",E),d.addEventListener("transitioncancel",le),_={onTransitionEnd:E,onTransitionCancel:le,fallbackTimer:Ke}}),this._activeRipples.set(b,_),(p||!l)&&this._finishRippleTransition(b),b}fadeOutRipple(n){if(n.state===It.FADING_OUT||n.state===It.HIDDEN)return;let e=n.element,i=y(y({},ls),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=It.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=dn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,Iy.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{My.forEach(e=>{this._triggerElement.addEventListener(e,this,ky)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===It.FADING_IN?this._startFadeOutTransition(n):n.state===It.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=It.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=It.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=Qo(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+ZM;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Ko(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===It.VISIBLE||n.config.terminateOnPointerUp&&n.state===It.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(Iy.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(My.forEach(e=>n.removeEventListener(e,this,ky)),this._pointerUpEventsRegistered=!1))}};function QM(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Lm=new g("mat-ripple-global-options"),el=(()=>{class t{_elementRef=f(O);_animationsDisabled=Fe();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=f(S),i=f(ve),r=f(Lm,{optional:!0}),o=f(Z);this._globalOptions=r||{},this._rippleRenderer=new ds(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:y(y(y({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,y(y({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,y(y({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&V("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var KM={capture:!0},XM=["focus","mousedown","mouseenter","touchstart"],Vm="mat-ripple-loader-uninitialized",Bm="mat-ripple-loader-class-name",Sy="mat-ripple-loader-centered",tl="mat-ripple-loader-disabled",Ty=(()=>{class t{_document=f(P);_animationsDisabled=Fe();_globalRippleOptions=f(Lm,{optional:!0});_platform=f(ve);_ngZone=f(S);_injector=f(Z);_eventCleanups;_hosts=new Map;constructor(){let e=f(Ve).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>XM.map(i=>e.listen(this._document,i,this._onInteraction,KM)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Vm,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Bm))&&e.setAttribute(Bm,i.className||""),i.centered&&e.setAttribute(Sy,""),i.disabled&&e.setAttribute(tl,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(tl,""):e.removeAttribute(tl)}_onInteraction=e=>{let i=$e(e);if(i instanceof HTMLElement){let r=i.closest(`[${Vm}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Bm)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??ls.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??ls.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(tl),rippleConfig:{centered:e.hasAttribute(Sy),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new ds(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(Vm)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ei=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2,changeDetection:0})}return t})();var JM=new g("MAT_BUTTON_CONFIG");function Ay(t){return t==null?void 0:ft(t)}var Ry=(()=>{class t{_elementRef=f(O);_ngZone=f(S);_animationsDisabled=Fe();_config=f(JM,{optional:!0});_focusMonitor=f(es);_cleanupClick;_renderer=f(qe);_rippleLoader=f(Ty);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){f(it).load(ei);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(ae("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),Wt(r.color?"mat-"+r.color:""),V("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",H],disabled:[2,"disabled","disabled",H],ariaDisabled:[2,"aria-disabled","ariaDisabled",H],disabledInteractive:[2,"disabledInteractive","disabledInteractive",H],tabIndex:[2,"tabIndex","tabIndex",Ay],_tabindex:[2,"tabindex","_tabindex",Ay]}})}return t})();var eS=["matButton",""],tS=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],nS=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var Oy=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Ny=(()=>{class t extends Ry{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=iS(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?Oy.get(this._appearance):null,o=Oy.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Me],attrs:eS,ngContentSelectors:nS,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Se(tS),wt(0,"span",0),Q(1),dt(2,"span",1),Q(3,1),xt(),Q(4,2),wt(5,"span",2)(6,"span",3)),i&2&&V("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2,changeDetection:0})}return t})();function iS(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}function rS(t,n){if(t&1){let e=Mn();w(0,"div",1)(1,"button",2),fe("click",function(){Pt(e);let r=U();return Lt(r.action())}),Ze(2),C()()}if(t&2){let e=U();x(2),Dt(" ",e.data.action," ")}}var oS=["label"];function sS(t,n){}var aS=Math.pow(2,31)-1,us=class{_overlayRef;instance;containerInstance;_afterDismissed=new D;_afterOpened=new D;_onAction=new D;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,aS))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},Fy=new g("MatSnackBarData"),Ur=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},cS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),lS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),dS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),uS=(()=>{class t{snackBarRef=f(us);data=f(Fy);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(w(0,"div",0),Ze(1),C(),ee(2,rS,3,1,"div",1)),i&2&&(x(),Dt(" ",r.data.message,`
`),x(),te(r.hasAction?2:-1))},dependencies:[Ny,cS,lS,dS],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),jm="_mat-snack-bar-enter",Hm="_mat-snack-bar-exit",fS=(()=>{class t extends Hr{_ngZone=f(S);_elementRef=f(O);_changeDetectorRef=f(ze);_platform=f(ve);_animationsDisabled=Fe();snackBarConfig=f(Ur);_document=f(P);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=f(Z);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new D;_onExit=new D;_onEnter=new D;_animationState="void";_live;_label;_role;_liveElementId=f(Ae).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===Hm?this._completeExit():e===jm&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?cn(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(jm)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(jm)},200)))}exit(){return this._destroyed?Xt(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?cn(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Hm)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(Hm),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&He(as,7)(oS,7),i&2){let o;$(o=W())&&(r._portalOutlet=o.first),$(o=W())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&fe("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&V("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[Me],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(w(0,"div",1)(1,"div",2,0)(3,"div",3),je(4,sS,0,0,"ng-template",4),C(),X(5,"div"),C()()),i&2&&(x(5),ae("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[as],styles:[`@keyframes _mat-snack-bar-enter {
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
`],encapsulation:2})}return t})(),zm=new g("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Ur}),Py=(()=>{class t{_live=f(ts);_injector=f(Z);_breakpointObserver=f(vm);_parentSnackBar=f(t,{optional:!0,skipSelf:!0});_defaultConfig=f(zm);_animationsDisabled=Fe();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=uS;snackBarContainerComponent=fS;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=y(y({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=Z.create({parent:r||this._injector,providers:[{provide:Ur,useValue:i}]}),s=new jr(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=y(y(y({},new Ur),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new us(s,o);if(e instanceof nt){let c=new An(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(c)}else{let c=this._createInjector(r,a),l=new jr(e,void 0,c),d=s.attachComponentPortal(l);a.instance=d.instance}return this._breakpointObserver.observe(Gv.HandsetPortrait).pipe(Pe(o.detachments())).subscribe(c=>{o.overlayElement.classList.toggle(this.handsetCssClass,c.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new Ki;i.direction=e.direction;let r=Nm(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,Xc(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return Z.create({parent:r||this._injector,providers:[{provide:us,useValue:i},{provide:Fy,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ly={providers:[tu(),{provide:Uc,useValue:{appearance:"outline",subscriptSizing:"dynamic"}},{provide:zm,useValue:{}}]};var mS=["*",[["mat-toolbar-row"]]],hS=["*","mat-toolbar-row"],pS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),Vy=(()=>{class t{_elementRef=f(O);_platform=f(ve);_document=f(P);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&$t(o,pS,5),i&2){let s;$(s=W())&&(r._toolbarRows=s)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(Wt(r.color?"mat-"+r.color:""),V("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:hS,decls:2,vars:0,template:function(i,r){i&1&&(Se(mS),Q(0),Q(1,1))},styles:[`.mat-toolbar {
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
`],encapsulation:2,changeDetection:0})}return t})();var fs=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new D;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var ms=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Gy=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(pe(qe),pe(O))};static \u0275dir=N({type:t})}return t})(),gS=(()=>{class t extends Gy{static \u0275fac=(()=>{let e;return function(r){return(e||(e=an(t)))(r||t)}})();static \u0275dir=N({type:t,features:[Me]})}return t})(),hl=new g("");var bS={provide:hl,useExisting:pt(()=>qy),multi:!0};function _S(){let t=Et()?Et().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var vS=new g(""),qy=(()=>{class t extends Gy{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!_S())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(pe(qe),pe(O),pe(vS,8))};static \u0275dir=N({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&fe("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[Te([bS]),Me]})}return t})();function Wm(t){return t==null||Gm(t)===0}function Gm(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var vs=new g(""),qm=new g(""),yS=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,il=class{static min(n){return xS(n)}static max(n){return wS(n)}static required(n){return DS(n)}static requiredTrue(n){return CS(n)}static email(n){return ES(n)}static minLength(n){return kS(n)}static maxLength(n){return IS(n)}static pattern(n){return MS(n)}static nullValidator(n){return Zy()}static compose(n){return ex(n)}static composeAsync(n){return tx(n)}};function xS(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function wS(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function DS(t){return Wm(t.value)?{required:!0}:null}function CS(t){return t.value===!0?null:{required:!0}}function ES(t){return Wm(t.value)||yS.test(t.value)?null:{email:!0}}function kS(t){return n=>{let e=n.value?.length??Gm(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function IS(t){return n=>{let e=n.value?.length??Gm(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function MS(t){if(!t)return Zy;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(Wm(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function Zy(t){return null}function Yy(t){return t!=null}function Qy(t){return Nr(t)?mt(t):t}function Ky(t){let n={};return t.forEach(e=>{n=e!=null?y(y({},n),e):n}),Object.keys(n).length===0?null:n}function Xy(t,n){return n.map(e=>e(t))}function SS(t){return!t.validate}function Jy(t){return t.map(n=>SS(n)?n:e=>n.validate(e))}function ex(t){if(!t)return null;let n=t.filter(Yy);return n.length==0?null:function(e){return Ky(Xy(e,n))}}function Zm(t){return t!=null?ex(Jy(t)):null}function tx(t){if(!t)return null;let n=t.filter(Yy);return n.length==0?null:function(e){let i=Xy(e,n).map(Qy);return Gl(i).pipe(ge(Ky))}}function Ym(t){return t!=null?tx(Jy(t)):null}function By(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function nx(t){return t._rawValidators}function ix(t){return t._rawAsyncValidators}function Um(t){return t?Array.isArray(t)?t:[t]:[]}function rl(t,n){return Array.isArray(t)?t.includes(n):t===n}function jy(t,n){let e=Um(n);return Um(t).forEach(r=>{rl(e,r)||e.push(r)}),e}function Hy(t,n){return Um(n).filter(e=>!rl(t,e))}var ol=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Zm(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Ym(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Xi=class extends ol{name;get formDirective(){return null}get path(){return null}},Ji=class extends ol{_parent=null;name=null;valueAccessor=null},$m=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var rx=(()=>{class t extends $m{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(pe(Ji,2))};static \u0275dir=N({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&V("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Me]})}return t})();var hs="VALID",nl="INVALID",$r="PENDING",ps="DISABLED",ti=class{},sl=class extends ti{value;source;constructor(n,e){super(),this.value=n,this.source=e}},bs=class extends ti{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},_s=class extends ti{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Wr=class extends ti{status;source;constructor(n,e){super(),this.status=n,this.source=e}},al=class extends ti{source;constructor(n){super(),this.source=n}},cl=class extends ti{source;constructor(n){super(),this.source=n}};function ox(t){return(pl(t)?t.validators:t)||null}function TS(t){return Array.isArray(t)?Zm(t):t||null}function sx(t,n){return(pl(n)?n.asyncValidators:t)||null}function AS(t){return Array.isArray(t)?Ym(t):t||null}function pl(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function RS(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new k(1e3,"");if(!i[e])throw new k(1001,"")}function OS(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new k(-1002,"")})}var ll=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return De(this.statusReactive)}set status(n){De(()=>this.statusReactive.set(n))}_status=_e(()=>this.statusReactive());statusReactive=G(void 0);get valid(){return this.status===hs}get invalid(){return this.status===nl}get pending(){return this.status===$r}get disabled(){return this.status===ps}get enabled(){return this.status!==ps}errors;get pristine(){return De(this.pristineReactive)}set pristine(n){De(()=>this.pristineReactive.set(n))}_pristine=_e(()=>this.pristineReactive());pristineReactive=G(!0);get dirty(){return!this.pristine}get touched(){return De(this.touchedReactive)}set touched(n){De(()=>this.touchedReactive.set(n))}_touched=_e(()=>this.touchedReactive());touchedReactive=G(!1);get untouched(){return!this.touched}_events=new D;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(jy(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(jy(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(Hy(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(Hy(n,this._rawAsyncValidators))}hasValidator(n){return rl(this._rawValidators,n)}hasAsyncValidator(n){return rl(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(B(y({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new _s(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new _s(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(B(y({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new bs(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new bs(!0,i))}markAsPending(n={}){this.status=$r;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Wr(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(B(y({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=ps,this.errors=null,this._forEachChild(r=>{r.disable(B(y({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new sl(this.value,i)),this._events.next(new Wr(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(B(y({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=hs,this._forEachChild(i=>{i.enable(B(y({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(B(y({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===hs||this.status===$r)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new sl(this.value,e)),this._events.next(new Wr(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(B(y({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?ps:hs}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=$r,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=Qy(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Wr(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new F,this.statusChanges=new F}_calculateStatus(){return this._allControlsDisabled()?ps:this.errors?nl:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus($r)?$r:this._anyControlsHaveStatus(nl)?nl:hs}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new bs(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new _s(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){pl(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=TS(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=AS(this._rawAsyncValidators)}},dl=class extends ll{constructor(n,e,i){super(ox(e),sx(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){OS(this,!0,n),Object.keys(n).forEach(i=>{RS(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,B(y({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new cl(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var gl=new g("",{factory:()=>Qm}),Qm="always";function NS(t,n){return[...n.path,t]}function ul(t,n,e=Qm){Km(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),PS(t,n),VS(t,n),LS(t,n),FS(t,n)}function zy(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),ml(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function fl(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function FS(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function Km(t,n){let e=nx(t);n.validator!==null?t.setValidators(By(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=ix(t);n.asyncValidator!==null?t.setAsyncValidators(By(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();fl(n._rawValidators,r),fl(n._rawAsyncValidators,r)}function ml(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=nx(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=ix(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return fl(n._rawValidators,i),fl(n._rawAsyncValidators,i),e}function PS(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&ax(t,n)})}function LS(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&ax(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function ax(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function VS(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function cx(t,n){t==null,Km(t,n)}function BS(t,n){return ml(t,n)}function jS(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function HS(t){return Object.getPrototypeOf(t.constructor)===gS}function lx(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function zS(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===qy?e=o:HS(o)?i=o:r=o}),r||i||e||null}function US(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var $S={provide:Xi,useExisting:pt(()=>Xm)},gs=Promise.resolve(),Xm=(()=>{class t extends Xi{callSetDisabledState;get submitted(){return De(this.submittedReactive)}_submitted=_e(()=>this.submittedReactive());submittedReactive=G(!1);_directives=new Set;form;ngSubmit=new F;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new dl({},Zm(e),Ym(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){gs.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),ul(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){gs.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){gs.then(()=>{let i=this._findContainer(e.path),r=new dl({});cx(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){gs.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){gs.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),lx(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new al(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(pe(vs,10),pe(qm,10),pe(gl,8))};static \u0275dir=N({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&fe("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Te([$S]),Me]})}return t})();function Uy(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function $y(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var dx=class extends ll{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(ox(e),sx(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),pl(e)&&(e.nonNullable||e.initialValueIsDefault)&&($y(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new cl(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){Uy(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){Uy(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){$y(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var WS=t=>t instanceof dx;var GS={provide:Ji,useExisting:pt(()=>Jm)},Wy=Promise.resolve(),Jm=(()=>{class t extends Ji{_changeDetectorRef;callSetDisabledState;control=new dx;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new F;constructor(e,i,r,o,s,a){super(),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=zS(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),jS(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){ul(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){Wy.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&H(i);Wy.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?NS(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(pe(Xi,9),pe(vs,10),pe(qm,10),pe(hl,10),pe(ze,8),pe(gl,8))};static \u0275dir=N({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Te([GS]),Me,Be]})}return t})();var qS=(()=>{class t extends Xi{callSetDisabledState;get submitted(){return De(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=_e(()=>this._submittedReactive());_submittedReactive=G(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(ml(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return ul(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){zy(e.control||null,e,!1),US(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,lx(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new al(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(zy(i||null,e),WS(r)&&(ul(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);cx(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&BS(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){Km(this.form,this),this._oldForm&&ml(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(pe(vs,10),pe(qm,10),pe(gl,8))};static \u0275dir=N({type:t,features:[Me,Be]})}return t})();var ZS={provide:Xi,useExisting:pt(()=>eh)},eh=(()=>{class t extends qS{form=null;ngSubmit=new F;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=an(t)))(r||t)}})();static \u0275dir=N({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&fe("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Te([ZS]),Me]})}return t})();var YS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=Ie({type:t});static \u0275inj=xe({})}return t})();var ux=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:gl,useValue:e.callSetDisabledState??Qm}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=Ie({type:t});static \u0275inj=xe({imports:[YS]})}return t})();var fx=(()=>{class t{_animationsDisabled=Fe();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&V("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
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
`],encapsulation:2,changeDetection:0})}return t})();var KS=["text"],XS=[[["mat-icon"]],"*"],JS=["mat-icon","*"];function eT(t,n){if(t&1&&X(0,"mat-pseudo-checkbox",1),t&2){let e=U();q("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function tT(t,n){if(t&1&&X(0,"mat-pseudo-checkbox",3),t&2){let e=U();q("disabled",e.disabled)}}function nT(t,n){if(t&1&&(w(0,"span",4),Ze(1),C()),t&2){let e=U();x(),Dt("(",e.group.label,")")}}var nh=new g("MAT_OPTION_PARENT_COMPONENT"),ih=new g("MatOptgroup");var th=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},bl=(()=>{class t{_element=f(O);_changeDetectorRef=f(ze);_parent=f(nh,{optional:!0});group=f(ih,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=f(Ae).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=G(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new F;_text;_stateChanges=new D;constructor(){let e=f(it);e.load(ei),e.load(_m),this._signalDisableRipple=!!this._parent&&Yn(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!kt(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new th(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&He(KS,7),i&2){let o;$(o=W())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&fe("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Qn("id",r.id),ae("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),V("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",H]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:JS,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Se(XS),ee(0,eT,1,2,"mat-pseudo-checkbox",1),Q(1),w(2,"span",2,0),Q(4,1),C(),ee(5,tT,1,1,"mat-pseudo-checkbox",3),ee(6,nT,2,1,"span",4),X(7,"div",5)),i&2&&(te(r.multiple?0:-1),x(5),te(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),x(),te(r.group&&r.group._inert?6:-1),x(),q("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[fx,el],styles:[`.mat-mdc-option {
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
`],encapsulation:2,changeDetection:0})}return t})();function mx(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function hx(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var px=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _l=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var iT=["trigger"],rT=["panel"],oT=[[["mat-select-trigger"]],"*"],sT=["mat-select-trigger","*"];function aT(t,n){if(t&1&&(w(0,"span",4),Ze(1),C()),t&2){let e=U();x(),Ui(e.placeholder)}}function cT(t,n){t&1&&Q(0)}function lT(t,n){if(t&1&&(w(0,"span",11),Ze(1),C()),t&2){let e=U(2);x(),Ui(e.triggerValue)}}function dT(t,n){if(t&1&&(w(0,"span",5),ee(1,cT,1,0)(2,lT,2,1,"span",11),C()),t&2){let e=U();x(),te(e.customTrigger?1:2)}}function uT(t,n){if(t&1){let e=Mn();w(0,"div",12,1),fe("keydown",function(r){Pt(e);let o=U();return Lt(o._handleKeydown(r))}),Q(2,1),C()}if(t&2){let e=U();Wt(e.panelClass),V("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),ae("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var fT=new g("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=f(Z);return()=>Kc(t)}}),mT=new g("MAT_SELECT_CONFIG"),gx=new g("MatSelectTrigger"),rh=class{source;value;constructor(n,e){this.source=n,this.value=e}},bx=(()=>{class t{_viewportRuler=f(os);_changeDetectorRef=f(ze);_elementRef=f(O);_dir=f(Jn,{optional:!0});_idGenerator=f(Ae);_renderer=f(qe);_parentFormField=f(zc,{optional:!0});ngControl=f(Ji,{self:!0,optional:!0});_liveAnnouncer=f(ts);_defaultOptions=f(mT,{optional:!0});_animationsDisabled=Fe();_popoverLocation;_initialized=new D;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=mx(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=hx(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new rh(this,e)}_scrollStrategyFactory=f(fT);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new D;_errorStateTracker;stateChanges=new D;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=G(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(il.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=Wl(()=>{let e=this.options;return e?e.changes.pipe(ht(e),pi(()=>pn(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(pi(()=>this.optionSelectionChanges))});openedChange=new F;_openedStream=this.openedChange.pipe(ye(e=>e),ge(()=>{}));_closedStream=this.openedChange.pipe(ye(e=>!e),ge(()=>{}));selectionChange=new F;valueChange=new F;constructor(){let e=f(px),i=f(Xm,{optional:!0}),r=f(eh,{optional:!0}),o=f(new Kn("tabindex"),{optional:!0}),s=f(cs,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new _l(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new fs(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(Pe(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(Pe(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(ht(null),Pe(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(hi(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&Dm(this._trackedModal,"aria-owns",i),zv(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;Dm(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!kt(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let c=this.selected;c&&a!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!kt(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(a?c.select():c.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!kt(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof zr?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new ns(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=pn(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(Pe(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),pn(...this.options.map(i=>i._stateChanges)).pipe(Pe(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=$e(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&$t(o,gx,5)(o,bl,5)(o,ih,5),i&2){let s;$(s=W())&&(r.customTrigger=s.first),$(s=W())&&(r.options=s),$(s=W())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&He(iT,5)(rT,5)(Jc,5),i&2){let o;$(o=W())&&(r.trigger=o.first),$(o=W())&&(r.panel=o.first),$(o=W())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&fe("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(ae("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),V("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",H],disableRipple:[2,"disableRipple","disableRipple",H],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:ft(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",H],placeholder:"placeholder",required:[2,"required","required",H],multiple:[2,"multiple","multiple",H],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",H],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",ft],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",H]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Te([{provide:Hc,useExisting:t},{provide:nh,useExisting:t}]),Be],ngContentSelectors:sT,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Se(oT),w(0,"div",2,0),fe("click",function(){return r.open()}),w(3,"div",3),ee(4,aT,2,1,"span",4)(5,dT,3,1,"span",5),C(),w(6,"div",6)(7,"div",7),Vt(),w(8,"svg",8),X(9,"path",9),C()()()(),je(10,uT,3,16,"ng-template",10),fe("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=ut(1);x(3),ae("id",r._valueId),x(),te(r.empty?4:5),x(6),q("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[zr,Jc],styles:[`@keyframes _mat-select-enter {
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
`],encapsulation:2,changeDetection:0})}return t})(),_x=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-select-trigger"]],features:[Te([{provide:gx,useExisting:t}])]})}return t})();function hT(t){return!!t&&Object.hasOwn(t,"selectedDocument")}var Zr=class t{_userData=G(this.loadUserData());userData=this._userData.asReadonly();loadUserData(){let n=localStorage.getItem("userData");if(!n)return null;try{let e=JSON.parse(n);if(hT(e))return e}catch{}return null}saveUserData(n){if(!n){localStorage.removeItem("userData");return}localStorage.setItem("userData",JSON.stringify(n)),this._userData.set(n)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})};function pT(t){return Array.isArray(t)&&t.every(({path:n,title:e})=>!!n&&(!!e||e?.trim()===""))}function vx(t,n,e){let i=structuredClone(t),r=i.variants?.map(o=>{let s=e.findIndex(({id:c})=>c===o);if(s===-1)return console.warn(`Document refers to non-existent or already claimed variant: ${i.id} => ${o}`),null;let a=e.splice(s,1)[0];return vx(a,-1,e)})?.filter(o=>!!o);return B(y({},i),{variants:r})}var vl=class t{_bossfights=Ev(()=>({url:"bossfights/manifest.json",reportProgress:!0}),{parse:n=>{if(!pT(n))throw Error("Manifest is not properly formatted");return structuredClone(n).map(vx).filter(Boolean)}});_rawArray=G(void 0);rawMetadata=this._rawArray.asReadonly();isLoading=this._bossfights.isLoading;error=this._bossfights.error;data=this._bossfights.value;reload(){this._bossfights.reload()}static \u0275fac=function(e){return new(e||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})};var gT=["determinateSpinner"];function bT(t,n){if(t&1&&(Vt(),w(0,"svg",11),X(1,"circle",12),C()),t&2){let e=U();ae("viewBox",e._viewBox()),x(),Sn("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),ae("r",e._circleRadius())}}var _T=new g("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:yx})}),yx=100,vT=10,xx=(()=>{class t{_elementRef=f(O);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=f(_T),i=km(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=yx;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-vT)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&He(gT,5),i&2){let o;$(o=W())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(ae("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),Wt("mat-"+r.color),Sn("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),V("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",ft],diameter:[2,"diameter","diameter",ft],strokeWidth:[2,"strokeWidth","strokeWidth",ft]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&(je(0,bT,2,8,"ng-template",null,0,$i),w(2,"div",2,1),Vt(),w(4,"svg",3),X(5,"circle",4),C()(),Dr(),w(6,"div",5)(7,"div",6)(8,"div",7),In(9,8),C(),w(10,"div",9),In(11,8),C(),w(12,"div",10),In(13,8),C()()()),i&2){let o=ut(1);x(4),ae("viewBox",r._viewBox()),x(),Sn("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),ae("r",r._circleRadius()),x(4),q("ngTemplateOutlet",o),x(2),q("ngTemplateOutlet",o),x(2),q("ngTemplateOutlet",o)}},dependencies:[Wi],styles:[`.mat-mdc-progress-spinner {
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
`],encapsulation:2,changeDetection:0})}return t})();function ch(){return{async:!1,breaks:!1,extensions:null,gfm:!0,hooks:null,pedantic:!1,renderer:null,silent:!1,tokenizer:null,walkTokens:null}}var nr=ch();function Sx(t){nr=t}var er={exec:()=>null};function ne(t,n=""){let e=typeof t=="string"?t:t.source,i={replace:(r,o)=>{let s=typeof o=="string"?o:o.source;return s=s.replace(Ye.caret,"$1"),e=e.replace(r,s),i},getRegex:()=>new RegExp(e,n)};return i}var yT=((t="")=>{try{return!!new RegExp("(?<=1)(?<!1)"+t)}catch{return!1}})(),Ye={codeRemoveIndent:/^(?: {1,4}| {0,3}\t)/gm,outputLinkReplace:/\\([\[\]])/g,indentCodeCompensation:/^(\s+)(?:```)/,beginningSpace:/^\s+/,endingHash:/#$/,startingSpaceChar:/^ /,endingSpaceChar:/ $/,nonSpaceChar:/[^ ]/,newLineCharGlobal:/\n/g,tabCharGlobal:/\t/g,multipleSpaceGlobal:/\s+/g,blankLine:/^[ \t]*$/,doubleBlankLine:/\n[ \t]*\n[ \t]*$/,blockquoteStart:/^ {0,3}>/,blockquoteSetextReplace:/\n {0,3}((?:=+|-+) *)(?=\n|$)/g,blockquoteSetextReplace2:/^ {0,3}>[ \t]?/gm,listReplaceNesting:/^ {1,4}(?=( {4})*[^ ])/g,listIsTask:/^\[[ xX]\] +\S/,listReplaceTask:/^\[[ xX]\] +/,listTaskCheckbox:/\[[ xX]\]/,anyLine:/\n.*\n/,hrefBrackets:/^<(.*)>$/,tableDelimiter:/[:|]/,tableAlignChars:/^\||\| *$/g,tableRowBlankLine:/\n[ \t]*$/,tableAlignRight:/^ *-+: *$/,tableAlignCenter:/^ *:-+: *$/,tableAlignLeft:/^ *:-+ *$/,startATag:/^<a /i,endATag:/^<\/a>/i,startPreScriptTag:/^<(pre|code|kbd|script)(\s|>)/i,endPreScriptTag:/^<\/(pre|code|kbd|script)(\s|>)/i,startAngleBracket:/^</,endAngleBracket:/>$/,pedanticHrefTitle:/^([^'"]*[^\s])\s+(['"])(.*)\2/,unicodeAlphaNumeric:/[\p{L}\p{N}]/u,escapeTest:/[&<>"']/,escapeReplace:/[&<>"']/g,escapeTestNoEncode:/[<>"']|&(?!(#\d{1,7}|#[Xx][a-fA-F0-9]{1,6}|\w+);)/,escapeReplaceNoEncode:/[<>"']|&(?!(#\d{1,7}|#[Xx][a-fA-F0-9]{1,6}|\w+);)/g,caret:/(^|[^\[])\^/g,percentDecode:/%25/g,findPipe:/\|/g,splitPipe:/ \|/,slashPipe:/\\\|/g,carriageReturn:/\r\n|\r/g,spaceLine:/^ +$/gm,notSpaceStart:/^\S*/,endingNewline:/\n$/,listItemRegex:t=>new RegExp(`^( {0,3}${t})((?:[	 ][^\\n]*)?(?:\\n|$))`),nextBulletRegex:t=>new RegExp(`^ {0,${Math.min(3,t-1)}}(?:[*+-]|\\d{1,9}[.)])((?:[ 	][^\\n]*)?(?:\\n|$))`),hrRegex:t=>new RegExp(`^ {0,${Math.min(3,t-1)}}((?:- *){3,}|(?:_ *){3,}|(?:\\* *){3,})(?:\\n+|$)`),fencesBeginRegex:t=>new RegExp(`^ {0,${Math.min(3,t-1)}}(?:\`\`\`|~~~)`),headingBeginRegex:t=>new RegExp(`^ {0,${Math.min(3,t-1)}}#`),htmlBeginRegex:t=>new RegExp(`^ {0,${Math.min(3,t-1)}}<(?:[a-z].*>|!--)`,"i"),blockquoteBeginRegex:t=>new RegExp(`^ {0,${Math.min(3,t-1)}}>`)},xT=/^(?:[ \t]*(?:\n|$))+/,wT=/^((?: {4}| {0,3}\t)[^\n]+(?:\n(?:[ \t]*(?:\n|$))*)?)+/,DT=/^ {0,3}(`{3,}(?=[^`\n]*(?:\n|$))|~{3,})([^\n]*)(?:\n|$)(?:|([\s\S]*?)(?:\n|$))(?: {0,3}\1[~`]* *(?=\n|$)|$)/,ws=/^ {0,3}((?:-[\t ]*){3,}|(?:_[ \t]*){3,}|(?:\*[ \t]*){3,})(?:\n+|$)/,CT=/^ {0,3}(#{1,6})(?=\s|$)(.*)(?:\n+|$)/,lh=/ {0,3}(?:[*+-]|\d{1,9}[.)])/,Tx=/^(?!bull |blockCode|fences|blockquote|heading|html|table)((?:.|\n(?!\s*?\n|bull |blockCode|fences|blockquote|heading|html|table))+?)\n {0,3}(=+|-+) *(?:\n+|$)/,Ax=ne(Tx).replace(/bull/g,lh).replace(/blockCode/g,/(?: {4}| {0,3}\t)/).replace(/fences/g,/ {0,3}(?:`{3,}|~{3,})/).replace(/blockquote/g,/ {0,3}>/).replace(/heading/g,/ {0,3}#{1,6}/).replace(/html/g,/ {0,3}<[^\n>]+>\n/).replace(/\|table/g,"").getRegex(),ET=ne(Tx).replace(/bull/g,lh).replace(/blockCode/g,/(?: {4}| {0,3}\t)/).replace(/fences/g,/ {0,3}(?:`{3,}|~{3,})/).replace(/blockquote/g,/ {0,3}>/).replace(/heading/g,/ {0,3}#{1,6}/).replace(/html/g,/ {0,3}<[^\n>]+>\n/).replace(/table/g,/ {0,3}\|?(?:[:\- ]*\|)+[\:\- ]*\n/).getRegex(),dh=/^([^\n]+(?:\n(?!hr|heading|lheading|blockquote|fences|list|html|table| +\n)[^\n]+)*)/,kT=/^[^\n]+/,uh=/(?!\s*\])(?:\\[\s\S]|[^\[\]\\])+/,IT=ne(/^ {0,3}\[(label)\]: *(?:\n[ \t]*)?([^<\s][^\s]*|<.*?>)(?:(?: +(?:\n[ \t]*)?| *\n[ \t]*)(title))? *(?:\n+|$)/).replace("label",uh).replace("title",/(?:"(?:\\"?|[^"\\])*"|'[^'\n]*(?:\n[^'\n]+)*\n?'|\([^()]*\))/).getRegex(),MT=ne(/^(bull)([ \t][^\n]+?)?(?:\n|$)/).replace(/bull/g,lh).getRegex(),Cl="address|article|aside|base|basefont|blockquote|body|caption|center|col|colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|footer|form|frame|frameset|h[1-6]|head|header|hr|html|iframe|legend|li|link|main|menu|menuitem|meta|nav|noframes|ol|optgroup|option|p|param|search|section|summary|table|tbody|td|tfoot|th|thead|title|tr|track|ul",fh=/<!--(?:-?>|[\s\S]*?(?:-->|$))/,ST=ne("^ {0,3}(?:<(script|pre|style|textarea)[\\s>][\\s\\S]*?(?:</\\1>[^\\n]*\\n+|$)|comment[^\\n]*(\\n+|$)|<\\?[\\s\\S]*?(?:\\?>\\n*|$)|<![A-Z][\\s\\S]*?(?:>\\n*|$)|<!\\[CDATA\\[[\\s\\S]*?(?:\\]\\]>\\n*|$)|</?(tag)(?: +|\\n|/?>)[\\s\\S]*?(?:(?:\\n[ 	]*)+\\n|$)|<(?!script|pre|style|textarea)([a-z][\\w-]*)(?:attribute)*? */?>(?=[ \\t]*(?:\\n|$))[\\s\\S]*?(?:(?:\\n[ 	]*)+\\n|$)|</(?!script|pre|style|textarea)[a-z][\\w-]*\\s*>(?=[ \\t]*(?:\\n|$))[\\s\\S]*?(?:(?:\\n[ 	]*)+\\n|$))","i").replace("comment",fh).replace("tag",Cl).replace("attribute",/ +[a-zA-Z:_][\w.:-]*(?: *= *"[^"\n]*"| *= *'[^'\n]*'| *= *[^\s"'=<>`]+)?/).getRegex(),Rx=ne(dh).replace("hr",ws).replace("heading"," {0,3}#{1,6}(?:\\s|$)").replace("|lheading","").replace("|table","").replace("blockquote"," {0,3}>").replace("fences"," {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list"," {0,3}(?:[*+-]|1[.)])[ \\t]").replace("html","</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag",Cl).getRegex(),TT=ne(/^( {0,3}> ?(paragraph|[^\n]*)(?:\n|$))+/).replace("paragraph",Rx).getRegex(),mh={blockquote:TT,code:wT,def:IT,fences:DT,heading:CT,hr:ws,html:ST,lheading:Ax,list:MT,newline:xT,paragraph:Rx,table:er,text:kT},wx=ne("^ *([^\\n ].*)\\n {0,3}((?:\\| *)?:?-+:? *(?:\\| *:?-+:? *)*(?:\\| *)?)(?:\\n((?:(?! *\\n|hr|heading|blockquote|code|fences|list|html).*(?:\\n|$))*)\\n*|$)").replace("hr",ws).replace("heading"," {0,3}#{1,6}(?:\\s|$)").replace("blockquote"," {0,3}>").replace("code","(?: {4}| {0,3}	)[^\\n]").replace("fences"," {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list"," {0,3}(?:[*+-]|1[.)])[ \\t]").replace("html","</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag",Cl).getRegex(),AT=B(y({},mh),{lheading:ET,table:wx,paragraph:ne(dh).replace("hr",ws).replace("heading"," {0,3}#{1,6}(?:\\s|$)").replace("|lheading","").replace("table",wx).replace("blockquote"," {0,3}>").replace("fences"," {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list"," {0,3}(?:[*+-]|1[.)])[ \\t]").replace("html","</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag",Cl).getRegex()}),RT=B(y({},mh),{html:ne(`^ *(?:comment *(?:\\n|\\s*$)|<(tag)[\\s\\S]+?</\\1> *(?:\\n{2,}|\\s*$)|<tag(?:"[^"]*"|'[^']*'|\\s[^'"/>\\s]*)*?/?> *(?:\\n{2,}|\\s*$))`).replace("comment",fh).replace(/tag/g,"(?!(?:a|em|strong|small|s|cite|q|dfn|abbr|data|time|code|var|samp|kbd|sub|sup|i|b|u|mark|ruby|rt|rp|bdi|bdo|span|br|wbr|ins|del|img)\\b)\\w+(?!:|[^\\w\\s@]*@)\\b").getRegex(),def:/^ *\[([^\]]+)\]: *<?([^\s>]+)>?(?: +(["(][^\n]+[")]))? *(?:\n+|$)/,heading:/^(#{1,6})(.*)(?:\n+|$)/,fences:er,lheading:/^(.+?)\n {0,3}(=+|-+) *(?:\n+|$)/,paragraph:ne(dh).replace("hr",ws).replace("heading",` *#{1,6} *[^
]`).replace("lheading",Ax).replace("|table","").replace("blockquote"," {0,3}>").replace("|fences","").replace("|list","").replace("|html","").replace("|tag","").getRegex()}),OT=/^\\([!"#$%&'()*+,\-./:;<=>?@\[\]\\^_`{|}~])/,NT=/^(`+)([^`]|[^`][\s\S]*?[^`])\1(?!`)/,Ox=/^( {2,}|\\)\n(?!\s*$)/,FT=/^(`+|[^`])(?:(?= {2,}\n)|[\s\S]*?(?:(?=[\\<!\[`*_]|\b_|$)|[^ ](?= {2,}\n)))/,Yr=/[\p{P}\p{S}]/u,El=/[\s\p{P}\p{S}]/u,hh=/[^\s\p{P}\p{S}]/u,PT=ne(/^((?![*_])punctSpace)/,"u").replace(/punctSpace/g,El).getRegex(),Nx=/(?!~)[\p{P}\p{S}]/u,LT=/(?!~)[\s\p{P}\p{S}]/u,VT=/(?:[^\s\p{P}\p{S}]|~)/u,BT=ne(/link|precode-code|html/,"g").replace("link",/\[(?:[^\[\]`]|(?<a>`+)[^`]+\k<a>(?!`))*?\]\((?:\\[\s\S]|[^\\\(\)]|\((?:\\[\s\S]|[^\\\(\)])*\))*\)/).replace("precode-",yT?"(?<!`)()":"(^^|[^`])").replace("code",/(?<b>`+)[^`]+\k<b>(?!`)/).replace("html",/<(?! )[^<>]*?>/).getRegex(),Fx=/^(?:\*+(?:((?!\*)punct)|([^\s*]))?)|^_+(?:((?!_)punct)|([^\s_]))?/,jT=ne(Fx,"u").replace(/punct/g,Yr).getRegex(),HT=ne(Fx,"u").replace(/punct/g,Nx).getRegex(),Px="^[^_*]*?__[^_*]*?\\*[^_*]*?(?=__)|[^*]+(?=[^*])|(?!\\*)punct(\\*+)(?=[\\s]|$)|notPunctSpace(\\*+)(?!\\*)(?=punctSpace|$)|(?!\\*)punctSpace(\\*+)(?=notPunctSpace)|[\\s](\\*+)(?!\\*)(?=punct)|(?!\\*)punct(\\*+)(?!\\*)(?=punct)|notPunctSpace(\\*+)(?=notPunctSpace)",zT=ne(Px,"gu").replace(/notPunctSpace/g,hh).replace(/punctSpace/g,El).replace(/punct/g,Yr).getRegex(),UT=ne(Px,"gu").replace(/notPunctSpace/g,VT).replace(/punctSpace/g,LT).replace(/punct/g,Nx).getRegex(),$T=ne("^[^_*]*?\\*\\*[^_*]*?_[^_*]*?(?=\\*\\*)|[^_]+(?=[^_])|(?!_)punct(_+)(?=[\\s]|$)|notPunctSpace(_+)(?!_)(?=punctSpace|$)|(?!_)punctSpace(_+)(?=notPunctSpace)|[\\s](_+)(?!_)(?=punct)|(?!_)punct(_+)(?!_)(?=punct)","gu").replace(/notPunctSpace/g,hh).replace(/punctSpace/g,El).replace(/punct/g,Yr).getRegex(),WT=ne(/^~~?(?:((?!~)punct)|[^\s~])/,"u").replace(/punct/g,Yr).getRegex(),GT="^[^~]+(?=[^~])|(?!~)punct(~~?)(?=[\\s]|$)|notPunctSpace(~~?)(?!~)(?=punctSpace|$)|(?!~)punctSpace(~~?)(?=notPunctSpace)|[\\s](~~?)(?!~)(?=punct)|(?!~)punct(~~?)(?!~)(?=punct)|notPunctSpace(~~?)(?=notPunctSpace)",qT=ne(GT,"gu").replace(/notPunctSpace/g,hh).replace(/punctSpace/g,El).replace(/punct/g,Yr).getRegex(),ZT=ne(/\\(punct)/,"gu").replace(/punct/g,Yr).getRegex(),YT=ne(/^<(scheme:[^\s\x00-\x1f<>]*|email)>/).replace("scheme",/[a-zA-Z][a-zA-Z0-9+.-]{1,31}/).replace("email",/[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+(@)[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+(?![-_])/).getRegex(),QT=ne(fh).replace("(?:-->|$)","-->").getRegex(),KT=ne("^comment|^</[a-zA-Z][\\w:-]*\\s*>|^<[a-zA-Z][\\w-]*(?:attribute)*?\\s*/?>|^<\\?[\\s\\S]*?\\?>|^<![a-zA-Z]+\\s[\\s\\S]*?>|^<!\\[CDATA\\[[\\s\\S]*?\\]\\]>").replace("comment",QT).replace("attribute",/\s+[a-zA-Z:_][\w.:-]*(?:\s*=\s*"[^"]*"|\s*=\s*'[^']*'|\s*=\s*[^\s"'=<>`]+)?/).getRegex(),xl=/(?:\[(?:\\[\s\S]|[^\[\]\\])*\]|\\[\s\S]|`+(?!`)[^`]*?`+(?!`)|``+(?=\])|[^\[\]\\`])*?/,XT=ne(/^!?\[(label)\]\(\s*(href)(?:(?:[ \t]+(?:\n[ \t]*)?|\n[ \t]*)(title))?\s*\)/).replace("label",xl).replace("href",/<(?:\\.|[^\n<>\\])+>|[^ \t\n\x00-\x1f]*/).replace("title",/"(?:\\"?|[^"\\])*"|'(?:\\'?|[^'\\])*'|\((?:\\\)?|[^)\\])*\)/).getRegex(),Lx=ne(/^!?\[(label)\]\[(ref)\]/).replace("label",xl).replace("ref",uh).getRegex(),Vx=ne(/^!?\[(ref)\](?:\[\])?/).replace("ref",uh).getRegex(),JT=ne("reflink|nolink(?!\\()","g").replace("reflink",Lx).replace("nolink",Vx).getRegex(),Dx=/[hH][tT][tT][pP][sS]?|[fF][tT][pP]/,ph={_backpedal:er,anyPunctuation:ZT,autolink:YT,blockSkip:BT,br:Ox,code:NT,del:er,delLDelim:er,delRDelim:er,emStrongLDelim:jT,emStrongRDelimAst:zT,emStrongRDelimUnd:$T,escape:OT,link:XT,nolink:Vx,punctuation:PT,reflink:Lx,reflinkSearch:JT,tag:KT,text:FT,url:er},eA=B(y({},ph),{link:ne(/^!?\[(label)\]\((.*?)\)/).replace("label",xl).getRegex(),reflink:ne(/^!?\[(label)\]\s*\[([^\]]*)\]/).replace("label",xl).getRegex()}),oh=B(y({},ph),{emStrongRDelimAst:UT,emStrongLDelim:HT,delLDelim:WT,delRDelim:qT,url:ne(/^((?:protocol):\/\/|www\.)(?:[a-zA-Z0-9\-]+\.?)+[^\s<]*|^email/).replace("protocol",Dx).replace("email",/[A-Za-z0-9._+-]+(@)[a-zA-Z0-9-_]+(?:\.[a-zA-Z0-9-_]*[a-zA-Z0-9])+(?![-_])/).getRegex(),_backpedal:/(?:[^?!.,:;*_'"~()&]+|\([^)]*\)|&(?![a-zA-Z0-9]+;$)|[?!.,:;*_'"~)]+(?!$))+/,del:/^(~~?)(?=[^\s~])((?:\\[\s\S]|[^\\])*?(?:\\[\s\S]|[^\s~\\]))\1(?=[^~]|$)/,text:ne(/^([`~]+|[^`~])(?:(?= {2,}\n)|(?=[a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-]+@)|[\s\S]*?(?:(?=[\\<!\[`*~_]|\b_|protocol:\/\/|www\.|$)|[^ ](?= {2,}\n)|[^a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-](?=[a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-]+@)))/).replace("protocol",Dx).getRegex()}),tA=B(y({},oh),{br:ne(Ox).replace("{2,}","*").getRegex(),text:ne(oh.text).replace("\\b_","\\b_| {2,}\\n").replace(/\{2,\}/g,"*").getRegex()}),yl={normal:mh,gfm:AT,pedantic:RT},ys={normal:ph,gfm:oh,breaks:tA,pedantic:eA},nA={"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"},Cx=t=>nA[t];function un(t,n){if(n){if(Ye.escapeTest.test(t))return t.replace(Ye.escapeReplace,Cx)}else if(Ye.escapeTestNoEncode.test(t))return t.replace(Ye.escapeReplaceNoEncode,Cx);return t}function Ex(t){try{t=encodeURI(t).replace(Ye.percentDecode,"%")}catch{return null}return t}function kx(t,n){let e=t.replace(Ye.findPipe,(o,s,a)=>{let c=!1,l=s;for(;--l>=0&&a[l]==="\\";)c=!c;return c?"|":" |"}),i=e.split(Ye.splitPipe),r=0;if(i[0].trim()||i.shift(),i.length>0&&!i.at(-1)?.trim()&&i.pop(),n)if(i.length>n)i.splice(n);else for(;i.length<n;)i.push("");for(;r<i.length;r++)i[r]=i[r].trim().replace(Ye.slashPipe,"|");return i}function ni(t,n,e){let i=t.length;if(i===0)return"";let r=0;for(;r<i;){let o=t.charAt(i-r-1);if(o===n&&!e)r++;else if(o!==n&&e)r++;else break}return t.slice(0,i-r)}function Ix(t){let n=t.split(`
`),e=n.length-1;for(;e>=0&&Ye.blankLine.test(n[e]);)e--;return n.length-e<=2?t:n.slice(0,e+1).join(`
`)}function iA(t,n){if(t.indexOf(n[1])===-1)return-1;let e=0;for(let i=0;i<t.length;i++)if(t[i]==="\\")i++;else if(t[i]===n[0])e++;else if(t[i]===n[1]&&(e--,e<0))return i;return e>0?-2:-1}function rA(t,n=0){let e=n,i="";for(let r of t)if(r==="	"){let o=4-e%4;i+=" ".repeat(o),e+=o}else i+=r,e++;return i}function Mx(t,n,e,i,r){let o=n.href,s=n.title||null,a=t[1].replace(r.other.outputLinkReplace,"$1");i.state.inLink=!0;let c={type:t[0].charAt(0)==="!"?"image":"link",raw:e,href:o,title:s,text:a,tokens:i.inlineTokens(a)};return i.state.inLink=!1,c}function oA(t,n,e){let i=t.match(e.other.indentCodeCompensation);if(i===null)return n;let r=i[1];return n.split(`
`).map(o=>{let s=o.match(e.other.beginningSpace);if(s===null)return o;let[a]=s;return a.length>=r.length?o.slice(r.length):o}).join(`
`)}var wl=class{options;rules;lexer;constructor(t){this.options=t||nr}space(t){let n=this.rules.block.newline.exec(t);if(n&&n[0].length>0)return{type:"space",raw:n[0]}}code(t){let n=this.rules.block.code.exec(t);if(n){let e=this.options.pedantic?n[0]:Ix(n[0]),i=e.replace(this.rules.other.codeRemoveIndent,"");return{type:"code",raw:e,codeBlockStyle:"indented",text:i}}}fences(t){let n=this.rules.block.fences.exec(t);if(n){let e=n[0],i=oA(e,n[3]||"",this.rules);return{type:"code",raw:e,lang:n[2]?n[2].trim().replace(this.rules.inline.anyPunctuation,"$1"):n[2],text:i}}}heading(t){let n=this.rules.block.heading.exec(t);if(n){let e=n[2].trim();if(this.rules.other.endingHash.test(e)){let i=ni(e,"#");(this.options.pedantic||!i||this.rules.other.endingSpaceChar.test(i))&&(e=i.trim())}return{type:"heading",raw:ni(n[0],`
`),depth:n[1].length,text:e,tokens:this.lexer.inline(e)}}}hr(t){let n=this.rules.block.hr.exec(t);if(n)return{type:"hr",raw:ni(n[0],`
`)}}blockquote(t){let n=this.rules.block.blockquote.exec(t);if(n){let e=ni(n[0],`
`).split(`
`),i="",r="",o=[];for(;e.length>0;){let s=!1,a=[],c;for(c=0;c<e.length;c++)if(this.rules.other.blockquoteStart.test(e[c]))a.push(e[c]),s=!0;else if(!s)a.push(e[c]);else break;e=e.slice(c);let l=a.join(`
`),d=l.replace(this.rules.other.blockquoteSetextReplace,`
    $1`).replace(this.rules.other.blockquoteSetextReplace2,"");i=i?`${i}
${l}`:l,r=r?`${r}
${d}`:d;let u=this.lexer.state.top;if(this.lexer.state.top=!0,this.lexer.blockTokens(d,o,!0),this.lexer.state.top=u,e.length===0)break;let m=o.at(-1);if(m?.type==="code")break;if(m?.type==="blockquote"){let h=m,p=h.raw+`
`+e.join(`
`),b=this.blockquote(p);o[o.length-1]=b,i=i.substring(0,i.length-h.raw.length)+b.raw,r=r.substring(0,r.length-h.text.length)+b.text;break}else if(m?.type==="list"){let h=m,p=h.raw+`
`+e.join(`
`),b=this.list(p);o[o.length-1]=b,i=i.substring(0,i.length-m.raw.length)+b.raw,r=r.substring(0,r.length-h.raw.length)+b.raw,e=p.substring(o.at(-1).raw.length).split(`
`);continue}}return{type:"blockquote",raw:i,tokens:o,text:r}}}list(t){let n=this.rules.block.list.exec(t);if(n){let e=n[1].trim(),i=e.length>1,r={type:"list",raw:"",ordered:i,start:i?+e.slice(0,-1):"",loose:!1,items:[]};e=i?`\\d{1,9}\\${e.slice(-1)}`:`\\${e}`,this.options.pedantic&&(e=i?e:"[*+-]");let o=this.rules.other.listItemRegex(e),s=!1;for(;t;){let c=!1,l="",d="";if(!(n=o.exec(t))||this.rules.block.hr.test(t))break;l=n[0],t=t.substring(l.length);let u=rA(n[2].split(`
`,1)[0],n[1].length),m=t.split(`
`,1)[0],h=!u.trim(),p=0;if(this.options.pedantic?(p=2,d=u.trimStart()):h?p=n[1].length+1:(p=u.search(this.rules.other.nonSpaceChar),p=p>4?1:p,d=u.slice(p),p+=n[1].length),h&&this.rules.other.blankLine.test(m)&&(l+=m+`
`,t=t.substring(m.length+1),c=!0),!c){let b=this.rules.other.nextBulletRegex(p),_=this.rules.other.hrRegex(p),E=this.rules.other.fencesBeginRegex(p),le=this.rules.other.headingBeginRegex(p),Ke=this.rules.other.htmlBeginRegex(p),Re=this.rules.other.blockquoteBeginRegex(p);for(;t;){let fn=t.split(`
`,1)[0],Mt;if(m=fn,this.options.pedantic?(m=m.replace(this.rules.other.listReplaceNesting,"  "),Mt=m):Mt=m.replace(this.rules.other.tabCharGlobal,"    "),E.test(m)||le.test(m)||Ke.test(m)||Re.test(m)||b.test(m)||_.test(m))break;if(Mt.search(this.rules.other.nonSpaceChar)>=p||!m.trim())d+=`
`+Mt.slice(p);else{if(h||u.replace(this.rules.other.tabCharGlobal,"    ").search(this.rules.other.nonSpaceChar)>=4||E.test(u)||le.test(u)||_.test(u))break;d+=`
`+m}h=!m.trim(),l+=fn+`
`,t=t.substring(fn.length+1),u=Mt.slice(p)}}r.loose||(s?r.loose=!0:this.rules.other.doubleBlankLine.test(l)&&(s=!0)),r.items.push({type:"list_item",raw:l,task:!!this.options.gfm&&this.rules.other.listIsTask.test(d),loose:!1,text:d,tokens:[]}),r.raw+=l}let a=r.items.at(-1);if(a)a.raw=a.raw.trimEnd(),a.text=a.text.trimEnd();else return;r.raw=r.raw.trimEnd();for(let c of r.items){this.lexer.state.top=!1,c.tokens=this.lexer.blockTokens(c.text,[]);let l=c.tokens[0];if(c.task&&(l?.type==="text"||l?.type==="paragraph")){c.text=c.text.replace(this.rules.other.listReplaceTask,""),l.raw=l.raw.replace(this.rules.other.listReplaceTask,""),l.text=l.text.replace(this.rules.other.listReplaceTask,"");for(let u=this.lexer.inlineQueue.length-1;u>=0;u--)if(this.rules.other.listIsTask.test(this.lexer.inlineQueue[u].src)){this.lexer.inlineQueue[u].src=this.lexer.inlineQueue[u].src.replace(this.rules.other.listReplaceTask,"");break}let d=this.rules.other.listTaskCheckbox.exec(c.raw);if(d){let u={type:"checkbox",raw:d[0]+" ",checked:d[0]!=="[ ]"};c.checked=u.checked,r.loose?c.tokens[0]&&["paragraph","text"].includes(c.tokens[0].type)&&"tokens"in c.tokens[0]&&c.tokens[0].tokens?(c.tokens[0].raw=u.raw+c.tokens[0].raw,c.tokens[0].text=u.raw+c.tokens[0].text,c.tokens[0].tokens.unshift(u)):c.tokens.unshift({type:"paragraph",raw:u.raw,text:u.raw,tokens:[u]}):c.tokens.unshift(u)}}else c.task&&(c.task=!1);if(!r.loose){let d=c.tokens.filter(m=>m.type==="space"),u=d.length>0&&d.some(m=>this.rules.other.anyLine.test(m.raw));r.loose=u}}if(r.loose)for(let c of r.items){c.loose=!0;for(let l of c.tokens)l.type==="text"&&(l.type="paragraph")}return r}}html(t){let n=this.rules.block.html.exec(t);if(n){let e=Ix(n[0]);return{type:"html",block:!0,raw:e,pre:n[1]==="pre"||n[1]==="script"||n[1]==="style",text:e}}}def(t){let n=this.rules.block.def.exec(t);if(n){let e=n[1].toLowerCase().replace(this.rules.other.multipleSpaceGlobal," "),i=n[2]?n[2].replace(this.rules.other.hrefBrackets,"$1").replace(this.rules.inline.anyPunctuation,"$1"):"",r=n[3]?n[3].substring(1,n[3].length-1).replace(this.rules.inline.anyPunctuation,"$1"):n[3];return{type:"def",tag:e,raw:ni(n[0],`
`),href:i,title:r}}}table(t){let n=this.rules.block.table.exec(t);if(!n||!this.rules.other.tableDelimiter.test(n[2]))return;let e=kx(n[1]),i=n[2].replace(this.rules.other.tableAlignChars,"").split("|"),r=n[3]?.trim()?n[3].replace(this.rules.other.tableRowBlankLine,"").split(`
`):[],o={type:"table",raw:ni(n[0],`
`),header:[],align:[],rows:[]};if(e.length===i.length){for(let s of i)this.rules.other.tableAlignRight.test(s)?o.align.push("right"):this.rules.other.tableAlignCenter.test(s)?o.align.push("center"):this.rules.other.tableAlignLeft.test(s)?o.align.push("left"):o.align.push(null);for(let s=0;s<e.length;s++)o.header.push({text:e[s],tokens:this.lexer.inline(e[s]),header:!0,align:o.align[s]});for(let s of r)o.rows.push(kx(s,o.header.length).map((a,c)=>({text:a,tokens:this.lexer.inline(a),header:!1,align:o.align[c]})));return o}}lheading(t){let n=this.rules.block.lheading.exec(t);if(n){let e=n[1].trim();return{type:"heading",raw:ni(n[0],`
`),depth:n[2].charAt(0)==="="?1:2,text:e,tokens:this.lexer.inline(e)}}}paragraph(t){let n=this.rules.block.paragraph.exec(t);if(n){let e=n[1].charAt(n[1].length-1)===`
`?n[1].slice(0,-1):n[1];return{type:"paragraph",raw:n[0],text:e,tokens:this.lexer.inline(e)}}}text(t){let n=this.rules.block.text.exec(t);if(n)return{type:"text",raw:n[0],text:n[0],tokens:this.lexer.inline(n[0])}}escape(t){let n=this.rules.inline.escape.exec(t);if(n)return{type:"escape",raw:n[0],text:n[1]}}tag(t){let n=this.rules.inline.tag.exec(t);if(n)return!this.lexer.state.inLink&&this.rules.other.startATag.test(n[0])?this.lexer.state.inLink=!0:this.lexer.state.inLink&&this.rules.other.endATag.test(n[0])&&(this.lexer.state.inLink=!1),!this.lexer.state.inRawBlock&&this.rules.other.startPreScriptTag.test(n[0])?this.lexer.state.inRawBlock=!0:this.lexer.state.inRawBlock&&this.rules.other.endPreScriptTag.test(n[0])&&(this.lexer.state.inRawBlock=!1),{type:"html",raw:n[0],inLink:this.lexer.state.inLink,inRawBlock:this.lexer.state.inRawBlock,block:!1,text:n[0]}}link(t){let n=this.rules.inline.link.exec(t);if(n){let e=n[2].trim();if(!this.options.pedantic&&this.rules.other.startAngleBracket.test(e)){if(!this.rules.other.endAngleBracket.test(e))return;let o=ni(e.slice(0,-1),"\\");if((e.length-o.length)%2===0)return}else{let o=iA(n[2],"()");if(o===-2)return;if(o>-1){let s=(n[0].indexOf("!")===0?5:4)+n[1].length+o;n[2]=n[2].substring(0,o),n[0]=n[0].substring(0,s).trim(),n[3]=""}}let i=n[2],r="";if(this.options.pedantic){let o=this.rules.other.pedanticHrefTitle.exec(i);o&&(i=o[1],r=o[3])}else r=n[3]?n[3].slice(1,-1):"";return i=i.trim(),this.rules.other.startAngleBracket.test(i)&&(this.options.pedantic&&!this.rules.other.endAngleBracket.test(e)?i=i.slice(1):i=i.slice(1,-1)),Mx(n,{href:i&&i.replace(this.rules.inline.anyPunctuation,"$1"),title:r&&r.replace(this.rules.inline.anyPunctuation,"$1")},n[0],this.lexer,this.rules)}}reflink(t,n){let e;if((e=this.rules.inline.reflink.exec(t))||(e=this.rules.inline.nolink.exec(t))){let i=(e[2]||e[1]).replace(this.rules.other.multipleSpaceGlobal," "),r=n[i.toLowerCase()];if(!r){let o=e[0].charAt(0);return{type:"text",raw:o,text:o}}return Mx(e,r,e[0],this.lexer,this.rules)}}emStrong(t,n,e=""){let i=this.rules.inline.emStrongLDelim.exec(t);if(!(!i||!i[1]&&!i[2]&&!i[3]&&!i[4]||i[4]&&e.match(this.rules.other.unicodeAlphaNumeric))&&(!(i[1]||i[3])||!e||this.rules.inline.punctuation.exec(e))){let r=[...i[0]].length-1,o,s,a=r,c=0,l=i[0][0]==="*"?this.rules.inline.emStrongRDelimAst:this.rules.inline.emStrongRDelimUnd;for(l.lastIndex=0,n=n.slice(-1*t.length+r);(i=l.exec(n))!==null;){if(o=i[1]||i[2]||i[3]||i[4]||i[5]||i[6],!o)continue;if(s=[...o].length,i[3]||i[4]){a+=s;continue}else if((i[5]||i[6])&&r%3&&!((r+s)%3)){c+=s;continue}if(a-=s,a>0)continue;s=Math.min(s,s+a+c);let d=[...i[0]][0].length,u=t.slice(0,r+i.index+d+s);if(Math.min(r,s)%2){let h=u.slice(1,-1);return{type:"em",raw:u,text:h,tokens:this.lexer.inlineTokens(h)}}let m=u.slice(2,-2);return{type:"strong",raw:u,text:m,tokens:this.lexer.inlineTokens(m)}}}}codespan(t){let n=this.rules.inline.code.exec(t);if(n){let e=n[2].replace(this.rules.other.newLineCharGlobal," "),i=this.rules.other.nonSpaceChar.test(e),r=this.rules.other.startingSpaceChar.test(e)&&this.rules.other.endingSpaceChar.test(e);return i&&r&&(e=e.substring(1,e.length-1)),{type:"codespan",raw:n[0],text:e}}}br(t){let n=this.rules.inline.br.exec(t);if(n)return{type:"br",raw:n[0]}}del(t,n,e=""){let i=this.rules.inline.delLDelim.exec(t);if(i&&(!i[1]||!e||this.rules.inline.punctuation.exec(e))){let r=[...i[0]].length-1,o,s,a=r,c=this.rules.inline.delRDelim;for(c.lastIndex=0,n=n.slice(-1*t.length+r);(i=c.exec(n))!==null;){if(o=i[1]||i[2]||i[3]||i[4]||i[5]||i[6],!o||(s=[...o].length,s!==r))continue;if(i[3]||i[4]){a+=s;continue}if(a-=s,a>0)continue;s=Math.min(s,s+a);let l=[...i[0]][0].length,d=t.slice(0,r+i.index+l+s),u=d.slice(r,-r);return{type:"del",raw:d,text:u,tokens:this.lexer.inlineTokens(u)}}}}autolink(t){let n=this.rules.inline.autolink.exec(t);if(n){let e,i;return n[2]==="@"?(e=n[1],i="mailto:"+e):(e=n[1],i=e),{type:"link",raw:n[0],text:e,href:i,tokens:[{type:"text",raw:e,text:e}]}}}url(t){let n;if(n=this.rules.inline.url.exec(t)){let e,i;if(n[2]==="@")e=n[0],i="mailto:"+e;else{let r;do r=n[0],n[0]=this.rules.inline._backpedal.exec(n[0])?.[0]??"";while(r!==n[0]);e=n[0],n[1]==="www."?i="http://"+n[0]:i=n[0]}return{type:"link",raw:n[0],text:e,href:i,tokens:[{type:"text",raw:e,text:e}]}}}inlineText(t){let n=this.rules.inline.text.exec(t);if(n){let e=this.lexer.state.inRawBlock;return{type:"text",raw:n[0],text:n[0],escaped:e}}}},Zt=class sh{tokens;options;state;inlineQueue;tokenizer;constructor(n){this.tokens=[],this.tokens.links=Object.create(null),this.options=n||nr,this.options.tokenizer=this.options.tokenizer||new wl,this.tokenizer=this.options.tokenizer,this.tokenizer.options=this.options,this.tokenizer.lexer=this,this.inlineQueue=[],this.state={inLink:!1,inRawBlock:!1,top:!0};let e={other:Ye,block:yl.normal,inline:ys.normal};this.options.pedantic?(e.block=yl.pedantic,e.inline=ys.pedantic):this.options.gfm&&(e.block=yl.gfm,this.options.breaks?e.inline=ys.breaks:e.inline=ys.gfm),this.tokenizer.rules=e}static get rules(){return{block:yl,inline:ys}}static lex(n,e){return new sh(e).lex(n)}static lexInline(n,e){return new sh(e).inlineTokens(n)}lex(n){n=n.replace(Ye.carriageReturn,`
`),this.blockTokens(n,this.tokens);for(let e=0;e<this.inlineQueue.length;e++){let i=this.inlineQueue[e];this.inlineTokens(i.src,i.tokens)}return this.inlineQueue=[],this.tokens}blockTokens(n,e=[],i=!1){this.tokenizer.lexer=this,this.options.pedantic&&(n=n.replace(Ye.tabCharGlobal,"    ").replace(Ye.spaceLine,""));let r=1/0;for(;n;){if(n.length<r)r=n.length;else{this.infiniteLoopError(n.charCodeAt(0));break}let o;if(this.options.extensions?.block?.some(a=>(o=a.call({lexer:this},n,e))?(n=n.substring(o.raw.length),e.push(o),!0):!1))continue;if(o=this.tokenizer.space(n)){n=n.substring(o.raw.length);let a=e.at(-1);o.raw.length===1&&a!==void 0?a.raw+=`
`:e.push(o);continue}if(o=this.tokenizer.code(n)){n=n.substring(o.raw.length);let a=e.at(-1);a?.type==="paragraph"||a?.type==="text"?(a.raw+=(a.raw.endsWith(`
`)?"":`
`)+o.raw,a.text+=`
`+o.text,this.inlineQueue.at(-1).src=a.text):e.push(o);continue}if(o=this.tokenizer.fences(n)){n=n.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.heading(n)){n=n.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.hr(n)){n=n.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.blockquote(n)){n=n.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.list(n)){n=n.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.html(n)){n=n.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.def(n)){n=n.substring(o.raw.length);let a=e.at(-1);a?.type==="paragraph"||a?.type==="text"?(a.raw+=(a.raw.endsWith(`
`)?"":`
`)+o.raw,a.text+=`
`+o.raw,this.inlineQueue.at(-1).src=a.text):this.tokens.links[o.tag]||(this.tokens.links[o.tag]={href:o.href,title:o.title},e.push(o));continue}if(o=this.tokenizer.table(n)){n=n.substring(o.raw.length),e.push(o);continue}if(o=this.tokenizer.lheading(n)){n=n.substring(o.raw.length),e.push(o);continue}let s=n;if(this.options.extensions?.startBlock){let a=1/0,c=n.slice(1),l;this.options.extensions.startBlock.forEach(d=>{l=d.call({lexer:this},c),typeof l=="number"&&l>=0&&(a=Math.min(a,l))}),a<1/0&&a>=0&&(s=n.substring(0,a+1))}if(this.state.top&&(o=this.tokenizer.paragraph(s))){let a=e.at(-1);i&&a?.type==="paragraph"?(a.raw+=(a.raw.endsWith(`
`)?"":`
`)+o.raw,a.text+=`
`+o.text,this.inlineQueue.pop(),this.inlineQueue.at(-1).src=a.text):e.push(o),i=s.length!==n.length,n=n.substring(o.raw.length);continue}if(o=this.tokenizer.text(n)){n=n.substring(o.raw.length);let a=e.at(-1);a?.type==="text"?(a.raw+=(a.raw.endsWith(`
`)?"":`
`)+o.raw,a.text+=`
`+o.text,this.inlineQueue.pop(),this.inlineQueue.at(-1).src=a.text):e.push(o);continue}if(n){this.infiniteLoopError(n.charCodeAt(0));break}}return this.state.top=!0,e}inline(n,e=[]){return this.inlineQueue.push({src:n,tokens:e}),e}inlineTokens(n,e=[]){this.tokenizer.lexer=this;let i=n,r=null;if(this.tokens.links){let l=Object.keys(this.tokens.links);if(l.length>0)for(;(r=this.tokenizer.rules.inline.reflinkSearch.exec(i))!==null;)l.includes(r[0].slice(r[0].lastIndexOf("[")+1,-1))&&(i=i.slice(0,r.index)+"["+"a".repeat(r[0].length-2)+"]"+i.slice(this.tokenizer.rules.inline.reflinkSearch.lastIndex))}for(;(r=this.tokenizer.rules.inline.anyPunctuation.exec(i))!==null;)i=i.slice(0,r.index)+"++"+i.slice(this.tokenizer.rules.inline.anyPunctuation.lastIndex);let o;for(;(r=this.tokenizer.rules.inline.blockSkip.exec(i))!==null;)o=r[2]?r[2].length:0,i=i.slice(0,r.index+o)+"["+"a".repeat(r[0].length-o-2)+"]"+i.slice(this.tokenizer.rules.inline.blockSkip.lastIndex);i=this.options.hooks?.emStrongMask?.call({lexer:this},i)??i;let s=!1,a="",c=1/0;for(;n;){if(n.length<c)c=n.length;else{this.infiniteLoopError(n.charCodeAt(0));break}s||(a=""),s=!1;let l;if(this.options.extensions?.inline?.some(u=>(l=u.call({lexer:this},n,e))?(n=n.substring(l.raw.length),e.push(l),!0):!1))continue;if(l=this.tokenizer.escape(n)){n=n.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.tag(n)){n=n.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.link(n)){n=n.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.reflink(n,this.tokens.links)){n=n.substring(l.raw.length);let u=e.at(-1);l.type==="text"&&u?.type==="text"?(u.raw+=l.raw,u.text+=l.text):e.push(l);continue}if(l=this.tokenizer.emStrong(n,i,a)){n=n.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.codespan(n)){n=n.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.br(n)){n=n.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.del(n,i,a)){n=n.substring(l.raw.length),e.push(l);continue}if(l=this.tokenizer.autolink(n)){n=n.substring(l.raw.length),e.push(l);continue}if(!this.state.inLink&&(l=this.tokenizer.url(n))){n=n.substring(l.raw.length),e.push(l);continue}let d=n;if(this.options.extensions?.startInline){let u=1/0,m=n.slice(1),h;this.options.extensions.startInline.forEach(p=>{h=p.call({lexer:this},m),typeof h=="number"&&h>=0&&(u=Math.min(u,h))}),u<1/0&&u>=0&&(d=n.substring(0,u+1))}if(l=this.tokenizer.inlineText(d)){n=n.substring(l.raw.length),l.raw.slice(-1)!=="_"&&(a=l.raw.slice(-1)),s=!0;let u=e.at(-1);u?.type==="text"?(u.raw+=l.raw,u.text+=l.text):e.push(l);continue}if(n){this.infiniteLoopError(n.charCodeAt(0));break}}return e}infiniteLoopError(n){let e="Infinite loop on byte: "+n;if(this.options.silent)console.error(e);else throw new Error(e)}},Dl=class{options;parser;constructor(t){this.options=t||nr}space(t){return""}code({text:t,lang:n,escaped:e}){let i=(n||"").match(Ye.notSpaceStart)?.[0],r=t.replace(Ye.endingNewline,"")+`
`;return i?'<pre><code class="language-'+un(i)+'">'+(e?r:un(r,!0))+`</code></pre>
`:"<pre><code>"+(e?r:un(r,!0))+`</code></pre>
`}blockquote({tokens:t}){return`<blockquote>
${this.parser.parse(t)}</blockquote>
`}html({text:t}){return t}def(t){return""}heading({tokens:t,depth:n}){return`<h${n}>${this.parser.parseInline(t)}</h${n}>
`}hr(t){return`<hr>
`}list(t){let n=t.ordered,e=t.start,i="";for(let s=0;s<t.items.length;s++){let a=t.items[s];i+=this.listitem(a)}let r=n?"ol":"ul",o=n&&e!==1?' start="'+e+'"':"";return"<"+r+o+`>
`+i+"</"+r+`>
`}listitem(t){return`<li>${this.parser.parse(t.tokens)}</li>
`}checkbox({checked:t}){return"<input "+(t?'checked="" ':"")+'disabled="" type="checkbox"> '}paragraph({tokens:t}){return`<p>${this.parser.parseInline(t)}</p>
`}table(t){let n="",e="";for(let r=0;r<t.header.length;r++)e+=this.tablecell(t.header[r]);n+=this.tablerow({text:e});let i="";for(let r=0;r<t.rows.length;r++){let o=t.rows[r];e="";for(let s=0;s<o.length;s++)e+=this.tablecell(o[s]);i+=this.tablerow({text:e})}return i&&(i=`<tbody>${i}</tbody>`),`<table>
<thead>
`+n+`</thead>
`+i+`</table>
`}tablerow({text:t}){return`<tr>
${t}</tr>
`}tablecell(t){let n=this.parser.parseInline(t.tokens),e=t.header?"th":"td";return(t.align?`<${e} align="${t.align}">`:`<${e}>`)+n+`</${e}>
`}strong({tokens:t}){return`<strong>${this.parser.parseInline(t)}</strong>`}em({tokens:t}){return`<em>${this.parser.parseInline(t)}</em>`}codespan({text:t}){return`<code>${un(t,!0)}</code>`}br(t){return"<br>"}del({tokens:t}){return`<del>${this.parser.parseInline(t)}</del>`}link({href:t,title:n,tokens:e}){let i=this.parser.parseInline(e),r=Ex(t);if(r===null)return i;t=r;let o='<a href="'+t+'"';return n&&(o+=' title="'+un(n)+'"'),o+=">"+i+"</a>",o}image({href:t,title:n,text:e,tokens:i}){i&&(e=this.parser.parseInline(i,this.parser.textRenderer));let r=Ex(t);if(r===null)return un(e);t=r;let o=`<img src="${t}" alt="${un(e)}"`;return n&&(o+=` title="${un(n)}"`),o+=">",o}text(t){return"tokens"in t&&t.tokens?this.parser.parseInline(t.tokens):"escaped"in t&&t.escaped?t.text:un(t.text)}},gh=class{strong({text:t}){return t}em({text:t}){return t}codespan({text:t}){return t}del({text:t}){return t}html({text:t}){return t}text({text:t}){return t}link({text:t}){return""+t}image({text:t}){return""+t}br(){return""}checkbox({raw:t}){return t}},Yt=class ah{options;renderer;textRenderer;constructor(n){this.options=n||nr,this.options.renderer=this.options.renderer||new Dl,this.renderer=this.options.renderer,this.renderer.options=this.options,this.renderer.parser=this,this.textRenderer=new gh}static parse(n,e){return new ah(e).parse(n)}static parseInline(n,e){return new ah(e).parseInline(n)}parse(n){this.renderer.parser=this;let e="";for(let i=0;i<n.length;i++){let r=n[i];if(this.options.extensions?.renderers?.[r.type]){let s=r,a=this.options.extensions.renderers[s.type].call({parser:this},s);if(a!==!1||!["space","hr","heading","code","table","blockquote","list","html","def","paragraph","text"].includes(s.type)){e+=a||"";continue}}let o=r;switch(o.type){case"space":{e+=this.renderer.space(o);break}case"hr":{e+=this.renderer.hr(o);break}case"heading":{e+=this.renderer.heading(o);break}case"code":{e+=this.renderer.code(o);break}case"table":{e+=this.renderer.table(o);break}case"blockquote":{e+=this.renderer.blockquote(o);break}case"list":{e+=this.renderer.list(o);break}case"checkbox":{e+=this.renderer.checkbox(o);break}case"html":{e+=this.renderer.html(o);break}case"def":{e+=this.renderer.def(o);break}case"paragraph":{e+=this.renderer.paragraph(o);break}case"text":{e+=this.renderer.text(o);break}default:{let s='Token with "'+o.type+'" type was not found.';if(this.options.silent)return console.error(s),"";throw new Error(s)}}}return e}parseInline(n,e=this.renderer){this.renderer.parser=this;let i="";for(let r=0;r<n.length;r++){let o=n[r];if(this.options.extensions?.renderers?.[o.type]){let a=this.options.extensions.renderers[o.type].call({parser:this},o);if(a!==!1||!["escape","html","link","image","strong","em","codespan","br","del","text"].includes(o.type)){i+=a||"";continue}}let s=o;switch(s.type){case"escape":{i+=e.text(s);break}case"html":{i+=e.html(s);break}case"link":{i+=e.link(s);break}case"image":{i+=e.image(s);break}case"checkbox":{i+=e.checkbox(s);break}case"strong":{i+=e.strong(s);break}case"em":{i+=e.em(s);break}case"codespan":{i+=e.codespan(s);break}case"br":{i+=e.br(s);break}case"del":{i+=e.del(s);break}case"text":{i+=e.text(s);break}default:{let a='Token with "'+s.type+'" type was not found.';if(this.options.silent)return console.error(a),"";throw new Error(a)}}}return i}},xs=class{options;block;constructor(t){this.options=t||nr}static passThroughHooks=new Set(["preprocess","postprocess","processAllTokens","emStrongMask"]);static passThroughHooksRespectAsync=new Set(["preprocess","postprocess","processAllTokens"]);preprocess(t){return t}postprocess(t){return t}processAllTokens(t){return t}emStrongMask(t){return t}provideLexer(t=this.block){return t?Zt.lex:Zt.lexInline}provideParser(t=this.block){return t?Yt.parse:Yt.parseInline}},sA=class{defaults=ch();options=this.setOptions;parse=this.parseMarkdown(!0);parseInline=this.parseMarkdown(!1);Parser=Yt;Renderer=Dl;TextRenderer=gh;Lexer=Zt;Tokenizer=wl;Hooks=xs;constructor(...t){this.use(...t)}walkTokens(t,n){let e=[];for(let i of t)switch(e=e.concat(n.call(this,i)),i.type){case"table":{let r=i;for(let o of r.header)e=e.concat(this.walkTokens(o.tokens,n));for(let o of r.rows)for(let s of o)e=e.concat(this.walkTokens(s.tokens,n));break}case"list":{let r=i;e=e.concat(this.walkTokens(r.items,n));break}default:{let r=i;this.defaults.extensions?.childTokens?.[r.type]?this.defaults.extensions.childTokens[r.type].forEach(o=>{let s=r[o].flat(1/0);e=e.concat(this.walkTokens(s,n))}):r.tokens&&(e=e.concat(this.walkTokens(r.tokens,n)))}}return e}use(...t){let n=this.defaults.extensions||{renderers:{},childTokens:{}};return t.forEach(e=>{let i=y({},e);if(i.async=this.defaults.async||i.async||!1,e.extensions&&(e.extensions.forEach(r=>{if(!r.name)throw new Error("extension name required");if("renderer"in r){let o=n.renderers[r.name];o?n.renderers[r.name]=function(...s){let a=r.renderer.apply(this,s);return a===!1&&(a=o.apply(this,s)),a}:n.renderers[r.name]=r.renderer}if("tokenizer"in r){if(!r.level||r.level!=="block"&&r.level!=="inline")throw new Error("extension level must be 'block' or 'inline'");let o=n[r.level];o?o.unshift(r.tokenizer):n[r.level]=[r.tokenizer],r.start&&(r.level==="block"?n.startBlock?n.startBlock.push(r.start):n.startBlock=[r.start]:r.level==="inline"&&(n.startInline?n.startInline.push(r.start):n.startInline=[r.start]))}"childTokens"in r&&r.childTokens&&(n.childTokens[r.name]=r.childTokens)}),i.extensions=n),e.renderer){let r=this.defaults.renderer||new Dl(this.defaults);for(let o in e.renderer){if(!(o in r))throw new Error(`renderer '${o}' does not exist`);if(["options","parser"].includes(o))continue;let s=o,a=e.renderer[s],c=r[s];r[s]=(...l)=>{let d=a.apply(r,l);return d===!1&&(d=c.apply(r,l)),d||""}}i.renderer=r}if(e.tokenizer){let r=this.defaults.tokenizer||new wl(this.defaults);for(let o in e.tokenizer){if(!(o in r))throw new Error(`tokenizer '${o}' does not exist`);if(["options","rules","lexer"].includes(o))continue;let s=o,a=e.tokenizer[s],c=r[s];r[s]=(...l)=>{let d=a.apply(r,l);return d===!1&&(d=c.apply(r,l)),d}}i.tokenizer=r}if(e.hooks){let r=this.defaults.hooks||new xs;for(let o in e.hooks){if(!(o in r))throw new Error(`hook '${o}' does not exist`);if(["options","block"].includes(o))continue;let s=o,a=e.hooks[s],c=r[s];xs.passThroughHooks.has(o)?r[s]=l=>{if(this.defaults.async&&xs.passThroughHooksRespectAsync.has(o))return(async()=>{let u=await a.call(r,l);return c.call(r,u)})();let d=a.call(r,l);return c.call(r,d)}:r[s]=(...l)=>{if(this.defaults.async)return(async()=>{let u=await a.apply(r,l);return u===!1&&(u=await c.apply(r,l)),u})();let d=a.apply(r,l);return d===!1&&(d=c.apply(r,l)),d}}i.hooks=r}if(e.walkTokens){let r=this.defaults.walkTokens,o=e.walkTokens;i.walkTokens=function(s){let a=[];return a.push(o.call(this,s)),r&&(a=a.concat(r.call(this,s))),a}}this.defaults=y(y({},this.defaults),i)}),this}setOptions(t){return this.defaults=y(y({},this.defaults),t),this}lexer(t,n){return Zt.lex(t,n??this.defaults)}parser(t,n){return Yt.parse(t,n??this.defaults)}parseMarkdown(t){return(n,e)=>{let i=y({},e),r=y(y({},this.defaults),i),o=this.onError(!!r.silent,!!r.async);if(this.defaults.async===!0&&i.async===!1)return o(new Error("marked(): The async option was set to true by an extension. Remove async: false from the parse options object to return a Promise."));if(typeof n>"u"||n===null)return o(new Error("marked(): input parameter is undefined or null"));if(typeof n!="string")return o(new Error("marked(): input parameter is of type "+Object.prototype.toString.call(n)+", string expected"));if(r.hooks&&(r.hooks.options=r,r.hooks.block=t),r.async)return(async()=>{let s=r.hooks?await r.hooks.preprocess(n):n,a=await(r.hooks?await r.hooks.provideLexer(t):t?Zt.lex:Zt.lexInline)(s,r),c=r.hooks?await r.hooks.processAllTokens(a):a;r.walkTokens&&await Promise.all(this.walkTokens(c,r.walkTokens));let l=await(r.hooks?await r.hooks.provideParser(t):t?Yt.parse:Yt.parseInline)(c,r);return r.hooks?await r.hooks.postprocess(l):l})().catch(o);try{r.hooks&&(n=r.hooks.preprocess(n));let s=(r.hooks?r.hooks.provideLexer(t):t?Zt.lex:Zt.lexInline)(n,r);r.hooks&&(s=r.hooks.processAllTokens(s)),r.walkTokens&&this.walkTokens(s,r.walkTokens);let a=(r.hooks?r.hooks.provideParser(t):t?Yt.parse:Yt.parseInline)(s,r);return r.hooks&&(a=r.hooks.postprocess(a)),a}catch(s){return o(s)}}}onError(t,n){return e=>{if(e.message+=`
Please report this to https://github.com/markedjs/marked.`,t){let i="<p>An error occurred:</p><pre>"+un(e.message+"",!0)+"</pre>";return n?Promise.resolve(i):i}if(n)return Promise.reject(e);throw e}}},tr=new sA;function se(t,n){return tr.parse(t,n)}se.options=se.setOptions=function(t){return tr.setOptions(t),se.defaults=tr.defaults,Sx(se.defaults),se};se.getDefaults=ch;se.defaults=nr;se.use=function(...t){return tr.use(...t),se.defaults=tr.defaults,Sx(se.defaults),se};se.walkTokens=function(t,n){return tr.walkTokens(t,n)};se.parseInline=tr.parseInline;se.Parser=Yt;se.parser=Yt.parse;se.Renderer=Dl;se.TextRenderer=gh;se.Lexer=Zt;se.lexer=Zt.lex;se.Tokenizer=wl;se.Hooks=xs;se.parse=se;var i9=se.options,r9=se.setOptions,o9=se.use,s9=se.walkTokens,a9=se.parseInline;var c9=Yt.parse,l9=Zt.lex;var kl=class t{SPOILER_PATTERN=/\|\|(.+?)\|\|/g;parseMarkdownDocument(n){let e=se.lexer(n),i=[],r="";for(let o of e)switch(o.type){case"heading":{let s=o;r=s.text;let a=s.raw.replace(/^#{1,6}\s+/,"");i.push({type:"heading",level:s.depth,html:se.parseInline(a),plainText:s.text});break}case"html":{i.push({type:"inline-html",html:o.raw});break}case"list":{let a=o.items.map((c,l)=>{let d=this.getRawText(c);return this.parseListItem(d,r,l)});i.push({type:"list",items:a});break}case"paragraph":{let s=o;i.push({type:"paragraph",html:se.parseInline(s.raw)});break}case"hr":case"thematic_break":{i.push({type:"thematic-break"});break}default:break}return i}getRawText(n){return n.tokens?n.tokens.map(e=>e.raw).join(""):n.raw||""}parseListItem(n,e,i){let r=[],o=n.matchAll(this.SPOILER_PATTERN);for(let c of o)r.push(c[1].replaceAll("`","").trim());let s=n.replace(this.SPOILER_PATTERN,"").trim();return s=s.replace(/\s+/g," ").trim(),{id:this.listItemId(e,s,i),text:s,spoilers:r}}listItemId(n,e,i){return`li-${this.hash(`${n}||${e}||${i}`)}`}hash(n){let e=5381;for(let i=0;i<n.length;i++)e=(e<<5)+e+n.charCodeAt(i),e=e&e;return Math.abs(e).toString(36).padStart(6,"0")}static \u0275fac=function(e){return new(e||t)};static \u0275prov=v({token:t,factory:t.\u0275fac,providedIn:"root"})};var bh=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=rs(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=rs(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(ae("aria-orientation",r.vertical?"vertical":"horizontal"),V("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
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
`],encapsulation:2,changeDetection:0})}return t})();var _h=new g("CdkAccordion"),Bx=(()=>{class t{_stateChanges=new D;_openCloseAllActions=new D;id=f(Ae).getId("cdk-accordion-");multi=!1;openAll(){this.multi&&this._openCloseAllActions.next(!0)}closeAll(){this._openCloseAllActions.next(!1)}ngOnChanges(e){this._stateChanges.next(e)}ngOnDestroy(){this._stateChanges.complete(),this._openCloseAllActions.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["cdk-accordion"],["","cdkAccordion",""]],inputs:{multi:[2,"multi","multi",H]},exportAs:["cdkAccordion"],features:[Te([{provide:_h,useExisting:t}]),Be]})}return t})(),jx=(()=>{class t{accordion=f(_h,{optional:!0,skipSelf:!0});_changeDetectorRef=f(ze);_expansionDispatcher=f(ms);_openCloseAllSubscription=ie.EMPTY;closed=new F;opened=new F;destroyed=new F;expandedChange=new F;id=f(Ae).getId("cdk-accordion-child-");get expanded(){return this._expanded}set expanded(e){if(this._expanded!==e){if(this._expanded=e,this.expandedChange.emit(e),e){this.opened.emit();let i=this.accordion?this.accordion.id:this.id;this._expansionDispatcher.notify(this.id,i)}else this.closed.emit();this._changeDetectorRef.markForCheck()}}_expanded=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=G(!1);_removeUniqueSelectionListener=()=>{};constructor(){}ngOnInit(){this._removeUniqueSelectionListener=this._expansionDispatcher.listen((e,i)=>{this.accordion&&!this.accordion.multi&&this.accordion.id===i&&this.id!==e&&(this.expanded=!1)}),this.accordion&&(this._openCloseAllSubscription=this._subscribeToOpenCloseAllActions())}ngOnDestroy(){this.opened.complete(),this.closed.complete(),this.destroyed.emit(),this.destroyed.complete(),this._removeUniqueSelectionListener(),this._openCloseAllSubscription.unsubscribe()}toggle(){this.disabled||(this.expanded=!this.expanded)}close(){this.disabled||(this.expanded=!1)}open(){this.disabled||(this.expanded=!0)}_subscribeToOpenCloseAllActions(){return this.accordion._openCloseAllActions.subscribe(e=>{this.disabled||(this.expanded=e)})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["cdk-accordion-item"],["","cdkAccordionItem",""]],inputs:{expanded:[2,"expanded","expanded",H],disabled:[2,"disabled","disabled",H]},outputs:{closed:"closed",opened:"opened",destroyed:"destroyed",expandedChange:"expandedChange"},exportAs:["cdkAccordionItem"],features:[Te([{provide:_h,useValue:void 0}])]})}return t})();var aA=["body"],cA=["bodyWrapper"],lA=[[["mat-expansion-panel-header"]],"*",[["mat-action-row"]]],dA=["mat-expansion-panel-header","*","mat-action-row"];function uA(t,n){}var fA=[[["mat-panel-title"]],[["mat-panel-description"]],"*"],mA=["mat-panel-title","mat-panel-description","*"];function hA(t,n){t&1&&(dt(0,"span",1),Vt(),dt(1,"svg",2),wt(2,"path",3),xt()())}var vh=new g("MAT_ACCORDION"),Hx=new g("MAT_EXPANSION_PANEL"),pA=(()=>{class t{_template=f(nt);_expansionPanel=f(Hx,{optional:!0});constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["ng-template","matExpansionPanelContent",""]]})}return t})(),zx=new g("MAT_EXPANSION_PANEL_DEFAULT_OPTIONS"),yh=(()=>{class t extends jx{_viewContainerRef=f(lt);_animationsDisabled=Fe();_document=f(P);_ngZone=f(S);_elementRef=f(O);_renderer=f(qe);_cleanupTransitionEnd;get hideToggle(){return this._hideToggle||this.accordion&&this.accordion.hideToggle}set hideToggle(e){this._hideToggle=e}_hideToggle=!1;get togglePosition(){return this._togglePosition||this.accordion&&this.accordion.togglePosition}set togglePosition(e){this._togglePosition=e}_togglePosition;afterExpand=new F;afterCollapse=new F;_inputChanges=new D;accordion=f(vh,{optional:!0,skipSelf:!0});_lazyContent;_body;_bodyWrapper;_portal;_headerId=f(Ae).getId("mat-expansion-panel-header-");constructor(){super();let e=f(zx,{optional:!0});this._expansionDispatcher=f(ms),e&&(this.hideToggle=e.hideToggle)}_hasSpacing(){return this.accordion?this.expanded&&this.accordion.displayMode==="default":!1}_getExpandedState(){return this.expanded?"expanded":"collapsed"}toggle(){this.expanded=!this.expanded}close(){this.expanded=!1}open(){this.expanded=!0}ngAfterContentInit(){this._lazyContent&&this._lazyContent._expansionPanel===this&&this.opened.pipe(ht(null),ye(()=>this.expanded&&!this._portal),hi(1)).subscribe(()=>{this._portal=new An(this._lazyContent._template,this._viewContainerRef)}),this._setupAnimationEvents()}ngOnChanges(e){this._inputChanges.next(e)}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransitionEnd?.(),this._inputChanges.complete()}_containsFocus(){if(this._body){let e=this._document.activeElement,i=this._body.nativeElement;return e===i||i.contains(e)}return!1}_transitionEndListener=({target:e,propertyName:i})=>{e===this._bodyWrapper?.nativeElement&&i==="grid-template-rows"&&this._ngZone.run(()=>{this.expanded?this.afterExpand.emit():this.afterCollapse.emit()})};_setupAnimationEvents(){this._ngZone.runOutsideAngular(()=>{this._animationsDisabled?(this.opened.subscribe(()=>this._ngZone.run(()=>this.afterExpand.emit())),this.closed.subscribe(()=>this._ngZone.run(()=>this.afterCollapse.emit()))):setTimeout(()=>{let e=this._elementRef.nativeElement;this._cleanupTransitionEnd=this._renderer.listen(e,"transitionend",this._transitionEndListener),e.classList.add("mat-expansion-panel-animations-enabled")},200)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-expansion-panel"]],contentQueries:function(i,r,o){if(i&1&&$t(o,pA,5),i&2){let s;$(s=W())&&(r._lazyContent=s.first)}},viewQuery:function(i,r){if(i&1&&He(aA,5)(cA,5),i&2){let o;$(o=W())&&(r._body=o.first),$(o=W())&&(r._bodyWrapper=o.first)}},hostAttrs:[1,"mat-expansion-panel"],hostVars:4,hostBindings:function(i,r){i&2&&V("mat-expanded",r.expanded)("mat-expansion-panel-spacing",r._hasSpacing())},inputs:{hideToggle:[2,"hideToggle","hideToggle",H],togglePosition:"togglePosition"},outputs:{afterExpand:"afterExpand",afterCollapse:"afterCollapse"},exportAs:["matExpansionPanel"],features:[Te([{provide:vh,useValue:void 0},{provide:Hx,useExisting:t}]),Me,Be],ngContentSelectors:dA,decls:9,vars:4,consts:[["bodyWrapper",""],["body",""],[1,"mat-expansion-panel-content-wrapper"],["role","region",1,"mat-expansion-panel-content",3,"id"],[1,"mat-expansion-panel-body"],[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(Se(lA),Q(0),w(1,"div",2,0)(3,"div",3,1)(5,"div",4),Q(6,1),je(7,uA,0,0,"ng-template",5),C(),Q(8,2),C()()),i&2&&(x(),ae("inert",r.expanded?null:""),x(2),q("id",r.id),ae("aria-labelledby",r._headerId),x(4),q("cdkPortalOutlet",r._portal))},dependencies:[as],styles:[`.mat-expansion-panel {
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
`],encapsulation:2,changeDetection:0})}return t})();var xh=(()=>{class t{panel=f(yh,{host:!0});_element=f(O);_focusMonitor=f(es);_changeDetectorRef=f(ze);_parentChangeSubscription=ie.EMPTY;constructor(){f(it).load(ei);let e=this.panel,i=f(zx,{optional:!0}),r=f(new Kn("tabindex"),{optional:!0}),o=e.accordion?e.accordion._stateChanges.pipe(ye(s=>!!(s.hideToggle||s.togglePosition))):fi;this.tabIndex=parseInt(r||"")||0,this._parentChangeSubscription=pn(e.opened,e.closed,o,e._inputChanges.pipe(ye(s=>!!(s.hideToggle||s.disabled||s.togglePosition)))).subscribe(()=>this._changeDetectorRef.markForCheck()),e.closed.pipe(ye(()=>e._containsFocus())).subscribe(()=>this._focusMonitor.focusVia(this._element,"program")),i&&(this.expandedHeight=i.expandedHeight,this.collapsedHeight=i.collapsedHeight)}expandedHeight;collapsedHeight;tabIndex=0;get disabled(){return this.panel.disabled}_toggle(){this.disabled||this.panel.toggle()}_isExpanded(){return this.panel.expanded}_getExpandedState(){return this.panel._getExpandedState()}_getPanelId(){return this.panel.id}_getTogglePosition(){return this.panel.togglePosition}_showToggle(){return!this.panel.hideToggle&&!this.panel.disabled}_getHeaderHeight(){let e=this._isExpanded();return e&&this.expandedHeight?this.expandedHeight:!e&&this.collapsedHeight?this.collapsedHeight:null}_keydown(e){switch(e.keyCode){case 32:case 13:kt(e)||(e.preventDefault(),this._toggle());break;default:this.panel.accordion&&this.panel.accordion._handleHeaderKeydown(e);return}}focus(e,i){e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}ngAfterViewInit(){this._focusMonitor.monitor(this._element).subscribe(e=>{e&&this.panel.accordion&&this.panel.accordion._handleHeaderFocus(this)})}ngOnDestroy(){this._parentChangeSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._element)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-expansion-panel-header"]],hostAttrs:["role","button",1,"mat-expansion-panel-header","mat-focus-indicator"],hostVars:13,hostBindings:function(i,r){i&1&&fe("click",function(){return r._toggle()})("keydown",function(s){return r._keydown(s)}),i&2&&(ae("id",r.panel._headerId)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r._getPanelId())("aria-expanded",r._isExpanded())("aria-disabled",r.panel.disabled),Sn("height",r._getHeaderHeight()),V("mat-expanded",r._isExpanded())("mat-expansion-toggle-indicator-after",r._getTogglePosition()==="after")("mat-expansion-toggle-indicator-before",r._getTogglePosition()==="before"))},inputs:{expandedHeight:"expandedHeight",collapsedHeight:"collapsedHeight",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:ft(e)]},ngContentSelectors:mA,decls:5,vars:3,consts:[[1,"mat-content"],[1,"mat-expansion-indicator"],["xmlns","http://www.w3.org/2000/svg","viewBox","0 -960 960 960","aria-hidden","true","focusable","false"],["d","M480-345 240-585l56-56 184 184 184-184 56 56-240 240Z"]],template:function(i,r){i&1&&(Se(fA),dt(0,"span",0),Q(1),Q(2,1),Q(3,2),xt(),ee(4,hA,3,0,"span",1)),i&2&&(V("mat-content-hide-toggle",!r._showToggle()),x(4),te(r._showToggle()?4:-1))},styles:[`.mat-expansion-panel-header {
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
`],encapsulation:2,changeDetection:0})}return t})();var Ux=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-panel-title"]],hostAttrs:[1,"mat-expansion-panel-header-title"]})}return t})(),$x=(()=>{class t extends Bx{_keyManager;_ownHeaders=new wn;_headers;hideToggle=!1;displayMode="default";togglePosition="after";ngAfterContentInit(){this._headers.changes.pipe(ht(this._headers)).subscribe(e=>{this._ownHeaders.reset(e.filter(i=>i.panel.accordion===this)),this._ownHeaders.notifyOnChanges()}),this._keyManager=new is(this._ownHeaders).withWrap().withHomeAndEnd()}_handleHeaderKeydown(e){this._keyManager.onKeydown(e)}_handleHeaderFocus(e){this._keyManager.updateActiveItem(e)}ngOnDestroy(){super.ngOnDestroy(),this._keyManager?.destroy(),this._ownHeaders.destroy()}static \u0275fac=(()=>{let e;return function(r){return(e||(e=an(t)))(r||t)}})();static \u0275dir=N({type:t,selectors:[["mat-accordion"]],contentQueries:function(i,r,o){if(i&1&&$t(o,xh,5),i&2){let s;$(s=W())&&(r._headers=s)}},hostAttrs:[1,"mat-accordion"],hostVars:2,hostBindings:function(i,r){i&2&&V("mat-accordion-multi",r.multi)},inputs:{hideToggle:[2,"hideToggle","hideToggle",H],displayMode:"displayMode",togglePosition:"togglePosition"},exportAs:["matAccordion"],features:[Te([{provide:vh,useExisting:t}]),Me]})}return t})();var gA=["mat-internal-form-field",""],bA=["*"],Wx=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&V("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:gA,ngContentSelectors:bA,decls:1,vars:0,template:function(i,r){i&1&&(Se(),Q(0))},styles:[`.mat-internal-form-field {
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
`],encapsulation:2,changeDetection:0})}return t})();var _A=["input"],vA=["label"],yA=["*"],wh={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},xA=new g("mat-checkbox-default-options",{providedIn:"root",factory:()=>wh}),Qe=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(Qe||{}),Dh=class{source;checked},Gx=(()=>{class t{_elementRef=f(O);_changeDetectorRef=f(ze);_ngZone=f(S);_animationsDisabled=Fe();_options=f(xA,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new Dh;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new F;indeterminateChange=new F;value;disableRipple=!1;_inputElement;_labelElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=Qe.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){f(it).load(ei);let e=f(new Kn("tabindex"),{optional:!0});this._options=this._options||wh,this.color=this._options.color||wh.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=f(Ae).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(Qe.Indeterminate):this._transitionCheckState(this.checked?Qe.Checked:Qe.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=G(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?Qe.Checked:Qe.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case Qe.Init:if(i===Qe.Checked)return this._animationClasses.uncheckedToChecked;if(i==Qe.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case Qe.Unchecked:return i===Qe.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case Qe.Checked:return i===Qe.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case Qe.Indeterminate:return i===Qe.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_onTouchTargetClick(){this._handleInputClick(),this.disabled||this._inputElement.nativeElement.focus()}_preventBubblingFromLabel(e){e.target&&this._labelElement.nativeElement.contains(e.target)&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=j({type:t,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&He(_A,5)(vA,5),i&2){let o;$(o=W())&&(r._inputElement=o.first),$(o=W())&&(r._labelElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(Qn("id",r.id),ae("tabindex",null)("aria-label",null)("aria-labelledby",null),Wt(r.color?"mat-"+r.color:"mat-accent"),V("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",H],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",H],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",H],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:ft(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",H],checked:[2,"checked","checked",H],disabled:[2,"disabled","disabled",H],indeterminate:[2,"indeterminate","indeterminate",H]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[Te([{provide:hl,useExisting:pt(()=>t),multi:!0},{provide:vs,useExisting:t,multi:!0}]),Be],ngContentSelectors:yA,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target",3,"click"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-label",3,"for"]],template:function(i,r){if(i&1&&(Se(),w(0,"div",3),fe("click",function(s){return r._preventBubblingFromLabel(s)}),w(1,"div",4,0)(3,"div",5),fe("click",function(){return r._onTouchTargetClick()}),C(),w(4,"input",6,1),fe("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(s){return r._onInteractionEvent(s)}),C(),X(6,"div",7),w(7,"div",8),Vt(),w(8,"svg",9),X(9,"path",10),C(),Dr(),X(10,"div",11),C(),X(11,"div",12),C(),w(12,"label",13,2),Q(14),C()()),i&2){let o=ut(2);q("labelPosition",r.labelPosition),x(4),V("mdc-checkbox--selected",r.checked),q("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),ae("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),x(7),q("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),x(),q("for",r.inputId)}},dependencies:[el,Wx],styles:[`.mdc-checkbox {
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
`],encapsulation:2,changeDetection:0})}return t})();function ir(t,n,e,i,r){if(t===n)return!e.strictZero||t!==0||1/t===1/n;if(typeof t=="number"&&typeof n=="number"&&isNaN(t)&&isNaN(n))return e.nanEqual;if((t===null||typeof t!="object"&&typeof t!="function")&&(n===null||typeof n!="object"&&typeof n!="function")||r>e.maxDepth||!t||!n||typeof t!="object"||typeof n!="object"||t.constructor!==n.constructor&&e.checkPrototypes)return!1;if(i.has(t))return i.get(t)===n;if(i.set(t,n),Array.isArray(t)){let o=t,s=n,a=o.length;if(a!==s.length)return!1;for(let c=a;c--!==0;)if(!ir(o[c],s[c],e,i,r+1))return!1;return!0}if(t.constructor===RegExp){let o=t,s=n;return o.source===s.source&&o.flags===s.flags}if(t.constructor===Date){let o=t,s=n;return o.getTime()===s.getTime()}if(t.constructor===ArrayBuffer){let o=t,s=n;if(o.byteLength!==s.byteLength)return!1;let a=new Uint8Array(o),c=new Uint8Array(s);for(let l=0;l<a.length;l++)if(a[l]!==c[l])return!1;return!0}if(ArrayBuffer.isView(t)){let o=t,s=n;if(o.constructor!==s.constructor||o.length!==s.length)return!1;for(let a=0;a<o.length;a++){let c=o[a],l=s[a];if(c!==l&&!(e.nanEqual&&typeof c=="number"&&typeof l=="number"&&Number.isNaN(c)&&Number.isNaN(l)))return!1}return!0}if(t.valueOf!==Object.prototype.valueOf)return t.valueOf()===n.valueOf();if(t.constructor===Error){let o=t,s=n;return o.name===s.name&&o.message===s.message}if(t.constructor===Set){let o=t,s=n;return o.size!==s.size?!1:wA(o,s,e,i,r)}if(t.constructor===Map){let o=t,s=n;return o.size!==s.size?!1:DA(o,s,e,i,r)}return CA(t,n,e,i,r)}function wA(t,n,e,i,r){if(t.size<=10){let c=new Set;for(let l of t){let d=!1,u=0;for(let m of n){if(!c.has(u)&&ir(l,m,e,new WeakMap,r+1)){c.add(u),d=!0;break}u++}if(!d)return!1}return!0}let o=Array.from(t),s=Array.from(n);if(o.length!==s.length)return!1;let a=new Set;for(let c=o.length;c--!==0;){let l=!1;for(let d=s.length;d--!==0;)if(!a.has(d)){let u=new WeakMap;if(ir(o[c],s[d],e,u,r+1)){a.add(d),l=!0;break}}if(!l)return!1}return!0}function DA(t,n,e,i,r){for(let[o,s]of t){let a=!1;for(let[c,l]of n)if(ir(o,c,e,i,r+1)&&ir(s,l,e,i,r+1)){a=!0;break}if(!a)return!1}return!0}function CA(t,n,e,i,r){let o=Object.keys(t),s=o.length;if(s!==Object.keys(n).length)return!1;for(let a=s;a--!==0;)if(!Object.prototype.hasOwnProperty.call(n,o[a]))return!1;for(let a=s;a--!==0;){let c=o[a];if(!ir(t[c],n[c],e,i,r+1))return!1}return!0}function EA(t,n,e={}){let i={nanEqual:e.nanEqual??!0,checkPrototypes:e.checkPrototypes??!1,strictZero:e.strictZero??!1,maxDepth:e.maxDepth??1e3};return ir(t,n,i,new WeakMap,0)}var qx=EA;var kA=(t,n)=>n.id;function IA(t,n){if(t&1&&X(0,"h1",5),t&2){let e=U(2).$implicit;q("innerHTML",e.plainText,Bi)}}function MA(t,n){if(t&1&&X(0,"h2",5),t&2){let e=U(2).$implicit;q("innerHTML",e.plainText,Bi)}}function SA(t,n){if(t&1&&(ee(0,IA,1,1,"h1",5),ee(1,MA,1,1,"h2",5)),t&2){let e=U().$implicit;te(e.level===1?0:-1),x(),te(e.level===2?1:-1)}}function TA(t,n){t&1&&X(0,"mat-divider")}function AA(t,n){if(t&1&&X(0,"p",2),t&2){let e=U().$implicit;q("innerHTML",e.html,Bi)}}function RA(t,n){if(t&1&&X(0,"div",3),t&2){let e=U().$implicit;q("innerHTML",e.html,Bi)}}function OA(t,n){if(t&1&&X(0,"h5",5),t&2){let e=n.$implicit;q("innerHTML",e,Bi)}}function NA(t,n){if(t&1){let e=Mn();w(0,"mat-expansion-panel",7),fe("expandedChange",function(r){let o=Pt(e).$implicit,s=U(3);return Lt(s.markArrowExpanded(o.id,r))}),w(1,"mat-expansion-panel-header")(2,"mat-panel-title",8)(3,"mat-checkbox",9),fe("click",function(r){return r.stopPropagation()})("keydown",function(r){return r.stopPropagation()})("change",function(r){let o=Pt(e).$implicit,s=U(3);return Lt(s.markArrowCompleted(o.id,r.checked))}),C(),Ze(4),C()(),w(5,"div",10),Hi(6,OA,1,1,"h5",5,fc),C()()}if(t&2){let e=n.$implicit,i=U(3);q("expanded",i.expandedNodes().has(e.id)),x(3),q("checked",i.completedArrows().has(e.id)),x(),Dt(" ",e.text," "),x(2),zi(e.spoilers)}}function FA(t,n){if(t&1&&(w(0,"mat-accordion",4),Hi(1,NA,8,3,"mat-expansion-panel",6,kA),C()),t&2){let e=U().$implicit;x(),zi(e.items)}}function PA(t,n){if(t&1&&ee(0,SA,2,2)(1,TA,1,0,"mat-divider")(2,AA,1,1,"p",2)(3,RA,1,1,"div",3)(4,FA,3,0,"mat-accordion",4),t&2){let e,i=n.$implicit;te((e=i.type)==="heading"?0:e==="thematic-break"?1:e==="paragraph"?2:e==="inline-html"?3:e==="list"?4:-1)}}var Il=class t{bossfightDocumentProcessor=f(kl);userDataService=f(Zr);userData=this.userDataService.userData;bossfightDocumentRecord=Y_.required({alias:"documentRecord"});bossfightMarkdown=Zf({params:()=>({documentUrl:this.bossfightDocumentRecord().path}),loader:({params:{documentUrl:n}})=>fetch(n).then(e=>e.text()),defaultValue:"# Loading"});blocks=_e(()=>this.bossfightDocumentProcessor.parseMarkdownDocument(this.bossfightMarkdown.value()??"# Loading"));expandedNodes=Ct({source:this.userData,computation:n=>{let r=((n?.documentStates??{})[this.bossfightDocumentRecord().id]??{}).readStates??{},o=new Set;return Object.entries(r).forEach(([s,{spoilerRevealed:a}])=>{a&&o.add(s)}),o}});completedArrows=Ct({source:this.userData,computation:n=>{let r=((n?.documentStates??{})[this.bossfightDocumentRecord().id]??{}).readStates??{},o=new Set;return Object.entries(r).forEach(([s,{arrowCompleted:a}])=>{a&&o.add(s)}),o}});updateUserData=tt(()=>{let n=this.bossfightDocumentRecord().id,e=structuredClone(this.userData())??{selectedDocument:n,documentStates:{[n]:{}}},i={},r=this.expandedNodes().intersection(this.completedArrows()),o=this.completedArrows().difference(this.expandedNodes()),s=this.expandedNodes().difference(this.completedArrows());r.forEach(a=>{i[a]={arrowCompleted:!0,spoilerRevealed:!0}}),o.forEach(a=>{i[a]={arrowCompleted:!0,spoilerRevealed:!1}}),s.forEach(a=>{i[a]={arrowCompleted:!1,spoilerRevealed:!0}}),e.documentStates??={},e.documentStates[n]??={},e.documentStates[n].readStates??={},!qx(e.documentStates[n].readStates,i)&&(e.documentStates[n].readStates=i,this.userDataService.saveUserData(e))});markArrowExpanded(n,e){this.expandedNodes.update(i=>{let r=new Set(i);return e?r.add(n):r.delete(n),r})}markArrowCompleted(n,e){this.completedArrows.update(i=>{let r=new Set(i);return e?r.add(n):r.delete(n),r})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=j({type:t,selectors:[["bossfight-render-component"]],inputs:{bossfightDocumentRecord:[1,"documentRecord","bossfightDocumentRecord"]},decls:4,vars:0,consts:[[1,"layout"],[1,"content"],[1,"mat-body-1",3,"innerHTML"],[1,"inline-html",3,"innerHTML"],["multi",""],[3,"innerHTML"],[1,"item-panel",3,"expanded"],[1,"item-panel",3,"expandedChange","expanded"],[1,"item-panel-title"],[3,"click","keydown","change","checked"],[1,"panel-content"]],template:function(e,i){e&1&&(w(0,"div",0)(1,"div",1),Hi(2,PA,5,1,null,null,fc),C()()),e&2&&(x(2),zi(i.blocks()))},dependencies:[bh,$x,yh,xh,Ux,Gx],styles:[".content[_ngcontent-%COMP%]{display:grid;justify-content:center}.content[_ngcontent-%COMP%]   .inline-html[_ngcontent-%COMP%]{width:fit-content;justify-self:center}.content[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%], .content[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%], .content[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%], .content[_ngcontent-%COMP%]   h4[_ngcontent-%COMP%], .content[_ngcontent-%COMP%]   h5[_ngcontent-%COMP%]{text-align:center}.content[_ngcontent-%COMP%]   .item-panel-title[_ngcontent-%COMP%]{gap:8px}"]})};var LA=()=>[],Zx=t=>({$implicit:t}),VA=(t,n)=>n==null?null:n.id;function BA(t,n){t&1&&(w(0,"div",1),X(1,"mat-progress-spinner",2),w(2,"h1"),Ze(3,"we loadin..."),C()())}function jA(t,n){t&1&&In(0)}function HA(t,n){if(t&1&&je(0,jA,1,0,"ng-container",11),t&2){U();let e=Uf(4);U();let i=ut(3);q("ngTemplateOutlet",i)("ngTemplateOutletContext",vc(2,Zx,e))}}function zA(t,n){t&1&&In(0)}function UA(t,n){if(t&1&&(w(0,"mat-option",7),je(1,zA,1,0,"ng-container",11),C()),t&2){let e=n.$implicit;U(2);let i=ut(3);q("value",e.id),x(),q("ngTemplateOutlet",i)("ngTemplateOutletContext",vc(3,Zx,e))}}function $A(t,n){if(t&1&&X(0,"bossfight-render-component",10),t&2){let e=U(2).selectedDocumentRecord();q("documentRecord",e)}}function WA(t,n){if(t&1){let e=Mn();w(0,"mat-toolbar",3)(1,"mat-form-field",4)(2,"mat-select",5),bc("ngModelChange",function(r){Pt(e);let o=U();return Hf(o.selectedBossfight,r)||(o.selectedBossfight=r),Lt(r)}),w(3,"mat-select-trigger",6),_c(4),ee(5,HA,1,4,"ng-container"),C(),Hi(6,UA,2,5,"mat-option",7,VA),C()(),X(8,"div",8),C(),w(9,"div",9),ee(10,$A,1,1,"bossfight-render-component",10),C()}if(t&2){let e=U();x(2),gc("ngModel",e.selectedBossfight),x(2);let i=zf(e.selectedDocumentRecord());x(),te(i?5:-1),x(),zi(e.bossfightData()??$f(4,LA)),x(4),te(e.selectedDocumentRecord()?10:-1)}}function GA(t,n){if(t&1&&(w(0,"span",12),X(1,"img",13),Ze(2),C()),t&2){let e=n.$implicit;x(),q("src",e.icon??"images/missing_icon.png",df),x(),Dt(" ",e.title," ")}}var Ml=class t{bossfightDataService=f(vl);userDataService=f(Zr);_snackBar=f(Py);snackBarRef=G(void 0);userData=this.userDataService.userData;bossfightDataLoading=this.bossfightDataService.isLoading;bossfightData=this.bossfightDataService.data;bossfightDataError=this.bossfightDataService.error;bossfightError=tt(()=>{let n=this.bossfightDataError();n?this.snackBarRef.set(this._snackBar.open(n.message,"Reload")):this._snackBar.dismiss()});snackBarActionSubscription=Ct({source:this.snackBarRef,computation:(n,e)=>(e&&e.value?.unsubscribe(),n?.onAction()?.subscribe(()=>this.bossfightDataService.reload()))});selectedBossfight=G("");initSelectedBossfight=tt(()=>{let n=this.userData();n?.selectedDocument&&this.selectedBossfight.set(n.selectedDocument),setTimeout(()=>this.initSelectedBossfight.destroy())});onSelectionChange=tt(()=>{let n=this.selectedBossfight(),e=this.userData();n&&e&&e.selectedDocument!==n&&(console.log("Updating user data with selected bossfight:",n),this.userDataService.saveUserData(B(y({},e),{selectedDocument:n})))});selectedDocumentRecord=_e(()=>{if(!this.bossfightDataLoading()&&this.bossfightData())return this.bossfightData()?.find(({id:n})=>n==this.selectedBossfight())});static \u0275fac=function(e){return new(e||t)};static \u0275cmp=j({type:t,selectors:[["app-root"]],decls:4,vars:1,consts:[["bossfightOptionContent",""],[1,"app-loading"],["mode","indeterminate"],[1,"mat-on-surface-variant"],["appearance","outline"],[3,"ngModelChange","ngModel"],[1,"bossfight-data-render"],[1,"bossfight-data-render",3,"value"],[1,"spacer"],[1,"content-container"],[3,"documentRecord"],[4,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"bossfight-data-wrapper"],["alt","",3,"src"]],template:function(e,i){e&1&&(ee(0,BA,4,0,"div",1)(1,WA,11,5),je(2,GA,3,2,"ng-template",null,0,$i)),e&2&&te(i.bossfightDataLoading()?0:1)},dependencies:[Vy,bx,bl,ux,rx,Jm,Sm,xx,_x,Wi,Il],styles:["[_nghost-%COMP%]{display:grid;grid-row:var(--mat-toolbar-standard-height) auto}mat-toolbar[_ngcontent-%COMP%]{position:fixed;padding-block:32px;padding-inline:16px;z-index:10}.content-container[_ngcontent-%COMP%]{padding-top:calc(var(--mat-toolbar-standard-height) + 16px)}mat-toolbar[_ngcontent-%COMP%]   mat-form-field[_ngcontent-%COMP%]{flex:1 0 auto}.app-loading[_ngcontent-%COMP%]{display:grid;width:100dvw;height:100dvh;align-content:center;justify-content:center}mat-select.bossfight-select[_ngcontent-%COMP%]{max-width:fit-content;min-width:56px}.bossfight-data-render[_ngcontent-%COMP%]   .bossfight-data-wrapper[_ngcontent-%COMP%]{display:inline-flex;flex-wrap:nowrap;align-items:center;height:32px;gap:8px}.bossfight-data-render[_ngcontent-%COMP%]   .bossfight-data-wrapper[_ngcontent-%COMP%]   img[_ngcontent-%COMP%]{height:32px}.bossfight-data-render[_ngcontent-%COMP%]   .bossfight-data-wrapper[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{overflow:hidden;text-overflow:ellipsis}"]})};am(Ml,Ly).catch(t=>console.error(t));
