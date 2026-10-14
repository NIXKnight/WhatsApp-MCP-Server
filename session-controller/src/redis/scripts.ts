/** Fixed source, no template interpolation of identities, clocks or lease data.
 * Decimal counters never pass through Lua tonumber (even epoch > 2^53).
 * Only bounded server milliseconds/lease durations are numeric.
 * Key types are exclusively owned by this single-instance namespace.
 */
export const SCRIPT = String.raw`
local op=ARGV[1]
local app,state,lease,stream,events,seqs,tools,checkpoint,identities=unpack(KEYS)
local expected={'hash','hash','hash','stream','hash','hash','hash','string','hash'}
for i,k in ipairs(KEYS) do
  local t=redis.call('TYPE',k).ok
  if t~='none' and t~=expected[i] then return redis.error_reply('store_unavailable') end
end
local function inc(s)
  local carry=1;local out=''
  for i=#s,1,-1 do local d=string.byte(s,i)-48+carry;if d==10 then d=0;carry=1 else carry=0 end;out=string.char(48+d)..out end
  if carry==1 then out='1'..out end
  return out
end
local function less(a,b) return #a<#b or (#a==#b and a<b) end
local function now() local t=redis.call('TIME');return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) end
local function alive(key,owner,epoch)
  return redis.call('HGET',key,'owner')==owner and redis.call('HGET',key,'epoch')==epoch and tonumber(redis.call('HGET',key,'until') or '0')>now()
end
local function fence()
  return alive(app,ARGV[2],ARGV[3]) and alive(lease,ARGV[4],ARGV[5]) and redis.call('HGET',lease,'appEpoch')==ARGV[3] and redis.call('HGET',lease,'appOwner')==ARGV[2]
end
local function work()
  if not fence() then return nil end
  local raw=redis.call('HGET',events,ARGV[6]);if not raw then return nil end
  local e=cjson.decode(raw)
  if e.id~=ARGV[7] or e.fp~=ARGV[8] or redis.call('HGET',lease,'event')~=ARGV[6] then return nil end
  return e
end
local function barrier() redis.call('HSET',state,'durability',inc(redis.call('HGET',state,'durability') or '0')) end
local function revision() redis.call('HSET',state,'revision',inc(redis.call('HGET',state,'revision') or '0')) end
local function renew(key,ms)
  local untilMs=now()+tonumber(ms);redis.call('HSET',key,'until',string.format('%.0f',untilMs));return {redis.call('HGET',key,'epoch'),string.format('%.0f',untilMs)}
end
if op=='init' then
  if redis.call('EXISTS',state)==0 then redis.call('HSET',state,'cursor','0','revision','0','chat',ARGV[2]) end
  if redis.call('HGET',state,'chat')~=ARGV[2] then return {'invalid'} end
  local r=redis.pcall('XGROUP','CREATE',stream,ARGV[3],'0','MKSTREAM')
  if type(r)=='table' and r.err and not string.find(r.err,'BUSYGROUP',1,true) then return redis.error_reply('store_unavailable') end
  barrier();return {'ok'}
elseif op=='snapshot' then
  return {redis.call('HGET',state,'revision') or '0',redis.call('HGET',state,'cursor') or '0',redis.call('HGET',app,'identityRevision') or '0'}
elseif op=='accept' then
  -- ARGV: op, revision, eventId, fingerprint, sequence, rawEvent, completed, canonicalChat
  if (redis.call('HGET',state,'revision') or '0')~=ARGV[2] or (redis.call('HGET',app,'identityRevision') or '0')~=ARGV[9] then return {'retry'} end
  local existing=redis.call('HGET',identities,ARGV[3])
  local identity=ARGV[8]..'|'..ARGV[4]
  if existing and existing~=identity then return {'conflict'} end
  local raw=redis.call('HGET',events,ARGV[3])
  if raw then
    local e=cjson.decode(raw)
    if e.fp~=ARGV[4] or e.seq~=ARGV[5] then return {'conflict'} end
    barrier();return {'duplicate'}
  end
  local other=redis.call('HGET',seqs,ARGV[5]);if other and other~=ARGV[3] then return {'conflict'} end
  if ARGV[7]=='yes' then barrier();return {'duplicate'} end
  local cursor=redis.call('HGET',state,'cursor') or '0'
  if not less(cursor,ARGV[5]) then return {'conflict'} end
  local id=redis.call('XADD',stream,'*','event',ARGV[3])
  redis.call('HSET',events,ARGV[3],cjson.encode({id=id,fp=ARGV[4],seq=ARGV[5],raw=ARGV[6]}))
  redis.call('HSET',seqs,ARGV[5],ARGV[3]);redis.call('HSET',identities,ARGV[3],identity)
  barrier();return {'accepted'}
elseif op=='app-acquire' then
  if tonumber(redis.call('HGET',app,'until') or '0')>now() then return {'busy'} end
  local epoch=inc(redis.call('HGET',app,'epoch') or '0');if #epoch>32 then return {'lost'} end
  redis.call('HSET',app,'owner',ARGV[2],'epoch',epoch);return renew(app,ARGV[3])
elseif op=='app-renew' then
  if not alive(app,ARGV[2],ARGV[3]) then return {'lost'} end
  return renew(app,ARGV[4])
elseif op=='app-release' then
  if not alive(app,ARGV[2],ARGV[3]) then return {'lost'} end
  redis.call('HDEL',app,'owner','until');return {'ok'}
elseif op=='claim' then
  -- op, appOwner, appEpoch, chatOwner, leaseMs, gapTimeout, group
  if not alive(app,ARGV[2],ARGV[3]) then return {'lost'} end
  if redis.call('HGET',state,'pause') then return {'paused'} end
  local sameApp=redis.call('HGET',lease,'appEpoch')==ARGV[3] and redis.call('HGET',lease,'appOwner')==ARGV[2]
  if sameApp and tonumber(redis.call('HGET',lease,'until') or '0')>now() then return {'waiting'} end
  -- Crash/lost-result attempts may never become automatic sends again.
  local journal=redis.call('HVALS',tools)
  local unresolved=false
  for _,raw in ipairs(journal) do
    local record=cjson.decode(raw);local t=record.state or record
    if t.status~='confirmed' then
      if record.state then
        t.status='uncertain';t.reason='mutation_uncertain'
        redis.call('HSET',tools,t.attemptId,cjson.encode(record))
      end
      unresolved=true
    end
  end
  if unresolved then redis.call('HSET',state,'pause','mutation_uncertain');barrier();return {'paused'} end
  local next=inc(redis.call('HGET',state,'cursor') or '0');local event=redis.call('HGET',seqs,next)
  if not event then
    if redis.call('HLEN',events)==0 then redis.call('HDEL',state,'gap');return {'empty'} end
    local gap=redis.call('HGET',state,'gap');if not gap then gap=string.format('%.0f',now());redis.call('HSET',state,'gap',gap) end
    if now()-tonumber(gap)>=tonumber(ARGV[6]) then redis.call('HSET',state,'pause','sequence_gap');barrier();return {'paused'} end
    barrier();return {'waiting'}
  end
  local raw=redis.call('HGET',events,event);if not raw then return {'lost'} end
  local e=cjson.decode(raw);local epoch=inc(redis.call('HGET',lease,'epoch') or '0');if #epoch>32 then return {'lost'} end
  -- FORCE creates/reclaims the precise next entry in the PEL, regardless of delivery order.
  local claimed=redis.call('XCLAIM',stream,ARGV[7],ARGV[4],0,e.id,'FORCE','JUSTID')
  if #claimed~=1 then return {'lost'} end
  redis.call('HSET',lease,'owner',ARGV[4],'epoch',epoch,'appOwner',ARGV[2],'appEpoch',ARGV[3],'event',event)
  redis.call('HDEL',state,'gap');local f=renew(lease,ARGV[5]);barrier();return {'claimed',e.id,e.raw,f[1],f[2]}
elseif op=='recover-pel' then
  -- App fencing and idle chat gate are atomic with Redis 7.2 XAUTOCLAIM.
  if not alive(app,ARGV[2],ARGV[3]) then return {'lost'} end
  local sameApp=redis.call('HGET',lease,'appEpoch')==ARGV[3] and redis.call('HGET',lease,'appOwner')==ARGV[2]
  if sameApp and tonumber(redis.call('HGET',lease,'until') or '0')>now() then return {'busy'} end
  if redis.call('HGET',state,'pause') then return {'paused'} end
  local r=redis.call('XAUTOCLAIM',stream,ARGV[7],ARGV[4],0,ARGV[5],'COUNT',ARGV[6],'JUSTID')
  -- Since Redis 7.0: cursor, claimed IDs, deleted IDs. Never authorize a turn here.
  if #r[3]>0 then redis.call('HSET',state,'pause','sequence_gap') end
  barrier();return {'ok',r[1],tostring(#r[2]),tostring(#r[3])}
elseif op=='check-work' then
  if not work() then return {'lost'} end
  barrier();return {'ok',redis.call('HGET',app,'until'),redis.call('HGET',lease,'until')}
elseif op=='turn-state' then
  if not work() then return {'lost'} end
  return {'ok',redis.call('HGET',state,'turnEvent') or ''}
elseif op=='begin-turn' then
  if not work() then return {'lost'} end
  if redis.call('HGET',state,'turnEvent') then return {'started'} end
  redis.call('HSET',state,'turnEvent',ARGV[6]);barrier();return {'ok'}
elseif op=='read' then
  return {redis.call('HGET',state,'cursor') or '0',redis.call('HGET',state,'session') or '',redis.call('HGET',state,'pause') or '',redis.call('HGET',state,'gap') or ''}
elseif op=='renew' then
  if not work() then return {'lost'} end
  local f=renew(lease,ARGV[9]);barrier();return f
elseif op=='pause' then
  if not work() then return {'lost'} end
  redis.call('HSET',state,'pause',ARGV[9]);barrier();return {'ok'}
elseif op=='bind' then
  if not work() then return {'lost'} end
  local old=redis.call('HGET',state,'session');if old and old~=ARGV[9] then return {'lost'} end
  redis.call('HSET',state,'session',ARGV[9]);barrier();return {'ok'}
elseif op=='complete' then
  local e=work();if not e then return {'lost'} end
  if e.seq~=inc(redis.call('HGET',state,'cursor') or '0') or redis.call('HGET',state,'session')~=ARGV[9] then return {'lost'} end
  local sends=cjson.decode(ARGV[11])
  for _,raw in ipairs(redis.call('HVALS',tools)) do
    local record=cjson.decode(raw);local t=record.state or record
    if t.status~='confirmed' or t.eventId~=ARGV[6] or (record.state and record.eventFingerprint~=ARGV[8]) then return {'lost'} end
    local matched=false;for _,sent in ipairs(sends) do if sent.destination==t.destination and sent.messageId==t.messageId then matched=true end end
    if not matched then return {'lost'} end
  end
  -- Caller has re-read strict durable Pi proof before this fenced transaction.
  redis.call('HSET',state,'cursor',e.seq);revision()
  redis.call('HSET',app,'identityRevision',inc(redis.call('HGET',app,'identityRevision') or '0'))
  redis.call('XACK',stream,ARGV[10],e.id);redis.call('XDEL',stream,e.id)
  redis.call('HDEL',events,ARGV[6]);redis.call('HDEL',seqs,e.seq);redis.call('HDEL',identities,ARGV[6]);redis.call('DEL',tools)
  redis.call('HDEL',lease,'owner','until','event');redis.call('HDEL',state,'turnEvent');barrier();return {'ok'}
elseif op=='journal-begin' then
  if not work() then return {'lost'} end
  local old=redis.call('HGET',tools,ARGV[9]);if old then return {'lost'} end
  redis.call('HSET',tools,ARGV[9],ARGV[10]);barrier();return {'ok'}
elseif op=='native-journal-begin' then
  if not work() then return {'lost'} end
  if redis.call('HGET',state,'turnEvent')~=ARGV[6] or redis.call('HGET',state,'session')~=ARGV[11] then return {'lost'} end
  local record=cjson.decode(ARGV[10]);local t=record.state
  if record.eventFingerprint~=ARGV[8] or t.eventId~=ARGV[6] or t.attemptId~=ARGV[9] or t.status~='attempting' then return {'lost'} end
  local old=redis.call('HGET',tools,ARGV[9]);if old then return {'lost'} end
  for _,raw in ipairs(redis.call('HVALS',tools)) do
    local r=cjson.decode(raw);local prior=r.state or r
    if prior.eventId==ARGV[6] and prior.toolCallId==t.toolCallId then return {'lost'} end
  end
  redis.call('HSET',tools,ARGV[9],ARGV[10]);barrier();return {'ok'}
elseif op=='journal-transition' then
  if not work() then return {'lost'} end
  local raw=redis.call('HGET',tools,ARGV[9]);if not raw then return {'lost'} end
  local record=cjson.decode(raw);local t=record.state or record
  if (record.state and record.eventFingerprint~=ARGV[8]) or t.eventId~=ARGV[6] or t.status~='attempting' then return {'lost'} end
  if ARGV[10]=='confirmed' then t.status='confirmed';t.messageId=ARGV[11];t.finishedAt=ARGV[12]
  else t.status='uncertain';t.reason='mutation_uncertain';redis.call('HSET',state,'pause','mutation_uncertain') end
  redis.call('HSET',tools,ARGV[9],cjson.encode(record));barrier();return {'ok'}
elseif op=='journal-list' then return redis.call('HVALS',tools)
elseif op=='checkpoint-prepare' then
  if not work() then return {'lost'} end
  local t=cjson.decode(ARGV[9]);local old=redis.call('GET',checkpoint)
  if old then
    local p=cjson.decode(old)
    if p.checkpointId==t.checkpointId then if old~=ARGV[9] then return {'lost'} end
    elseif p.phase~='restored' then return {'lost'} end
  end
  redis.call('SET',checkpoint,ARGV[9]);barrier();return {'ok'}
elseif op=='checkpoint-transition' then
  if not work() then return {'lost'} end
  local raw=redis.call('GET',checkpoint);if not raw then return {'lost'} end
  local t=cjson.decode(raw);if t.checkpointId~=ARGV[9] then return {'lost'} end
  if ARGV[10]=='compacted' and t.phase~='prepared' or ARGV[10]=='restored' and (t.phase~='compacted' or t.compactionEntryId~=ARGV[11]) then return {'lost'} end
  if raw~=ARGV[12] then return {'lost'} end
  redis.call('SET',checkpoint,ARGV[13]);barrier();return {'ok'}
elseif op=='checkpoint-latest' then return {redis.call('GET',checkpoint) or ''}
end
return redis.error_reply('store_unavailable')
`;
