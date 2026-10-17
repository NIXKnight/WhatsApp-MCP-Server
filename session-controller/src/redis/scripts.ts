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
local function fields(t,n)
  if type(t)~='table' then return false end
  local count=0;for k,_ in pairs(t) do if type(k)~='string' then return false end;count=count+1 end
  return count==n
end
local function sameBinding(a,b)
  return fields(a,3) and fields(b,3) and type(a.canonicalChatId)=='string' and
    type(a.sessionId)=='string' and type(a.sessionFile)=='string' and
    a.canonicalChatId==b.canonicalChatId and a.sessionId==b.sessionId and a.sessionFile==b.sessionFile
end
local function validGeneration(g)
  return fields(g,5) and type(g.device)=='string' and type(g.inode)=='string' and
    type(g.size)=='string' and type(g.mtimeNs)=='string' and type(g.ctimeNs)=='string'
end
local function repairEvidence(chat,group,intent)
  if redis.call('EXISTS',state)==0 or redis.call('HGET',state,'chat')~=chat then return nil,'ineligible' end
  if redis.call('HGET',state,'pause')~='compaction_failed' then return nil,'ineligible' end
  local rawCheckpoint=redis.call('GET',checkpoint);if not rawCheckpoint then return nil,'ineligible' end
  local prepared=cjson.decode(rawCheckpoint)
  if not fields(prepared,prepared.recovery and 4 or 3) or prepared.phase~='prepared' or
      type(prepared.checkpointId)~='string' or type(prepared.checkpoint)~='table' or
      prepared.checkpoint.canonicalChatId~=chat then return nil,'ineligible' end
  if redis.call('HGET',state,'turnEvent') or redis.call('HLEN',tools)~=0 then return nil,'unsafe' end
  local sessionRaw=redis.call('HGET',state,'session');if not sessionRaw then return nil,'unsafe' end
  local okSession,session=pcall(cjson.decode,sessionRaw);if not okSession or not fields(session,3) or session.canonicalChatId~=chat then return nil,'unsafe' end
  local next=inc(redis.call('HGET',state,'cursor') or '0');local event=redis.call('HGET',seqs,next)
  if not event then return nil,'unsafe' end
  local raw=redis.call('HGET',events,event);if not raw then return nil,'unsafe' end
  local e=cjson.decode(raw);if e.seq~=next or not e.id or not e.fp or not e.raw then return nil,'unsafe' end
  local rows=redis.call('XRANGE',stream,e.id,e.id,'COUNT',2)
  if #rows~=1 or #rows[1]~=2 or #rows[1][2]~=2 or rows[1][2][1]~='event' or rows[1][2][2]~=event then return nil,'unsafe' end
  local pending=redis.call('XPENDING',stream,group,e.id,e.id,2)
  if #pending~=1 or pending[1][1]~=e.id then return nil,'unsafe' end
  if prepared.recovery then
    local recovery=prepared.recovery
    if not fields(recovery,6) or recovery.version~=1 or recovery.eventId~=event or
        recovery.eventFingerprint~=e.fp or recovery.streamId~=e.id or
        not sameBinding(recovery.session,session) or not validGeneration(recovery.generation) then return nil,'unsafe' end
  else
    if intent=='@generate' then return {event,e,sessionRaw,rawCheckpoint,prepared},nil end
    if intent=='' then return nil,'intent_required' end
  end
  return {event,e,sessionRaw,rawCheckpoint,prepared},nil
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
  -- Redis 7.2: cursor, claimed IDs, deleted IDs. JUSTID preserves delivery
  -- counts, and no returned ID grants execution authority.
  if #r~=3 or type(r[2])~='table' or type(r[3])~='table' then return redis.error_reply('store_unavailable') end
  if #r[3]>0 then redis.call('HSET',state,'pause','sequence_gap') end
  barrier();return {'ok',r[1],tostring(#r[2]),tostring(#r[3])}
elseif op=='repair-intent' then
  if not alive(app,ARGV[2],ARGV[3]) then return {'lost'} end
  local sameApp=redis.call('HGET',lease,'appEpoch')==ARGV[3] and redis.call('HGET',lease,'appOwner')==ARGV[2]
  if sameApp and tonumber(redis.call('HGET',lease,'until') or '0')>now() then return {'busy'} end
  local proof,reason=repairEvidence(ARGV[7],ARGV[6],'@generate');if not proof then return {reason} end
  if proof[5].recovery then return {'ineligible'} end
  local epoch=inc(redis.call('HGET',lease,'epoch') or '0');if #epoch>32 then return {'lost'} end
  redis.call('HSET',lease,'owner',ARGV[4],'epoch',epoch,'appOwner',ARGV[2],'appEpoch',ARGV[3],'event',proof[1])
  redis.call('HDEL',lease,'repairIntent')
  local f=renew(lease,ARGV[5]);barrier()
  return {'claimed',proof[2].id,proof[2].raw,proof[3],proof[4],f[1],f[2]}
elseif op=='repair-claim' then
  -- Recovery owns the application lease. No stream/PEL command here changes
  -- delivery owner/count/idle metadata; XPENDING is evidence only.
  if not alive(app,ARGV[2],ARGV[3]) then return {'lost'} end
  local sameApp=redis.call('HGET',lease,'appEpoch')==ARGV[3] and redis.call('HGET',lease,'appOwner')==ARGV[2]
  if sameApp and tonumber(redis.call('HGET',lease,'until') or '0')>now() then return {'busy'} end
  local proof,reason=repairEvidence(ARGV[7],ARGV[6],ARGV[8]);if not proof then return {reason} end
  local event,e,session,rawCheckpoint=proof[1],proof[2],proof[3],proof[4]
  local epoch=inc(redis.call('HGET',lease,'epoch') or '0');if #epoch>32 then return {'lost'} end
  redis.call('HSET',lease,'owner',ARGV[4],'epoch',epoch,'appOwner',ARGV[2],'appEpoch',ARGV[3],'event',event)
  if proof[5].recovery then redis.call('HDEL',lease,'repairIntent') else redis.call('HSET',lease,'repairIntent',ARGV[8]) end
  local f=renew(lease,ARGV[5]);barrier()
  return {'claimed',e.id,e.raw,session,rawCheckpoint,f[1],f[2]}
elseif op=='repair-release-fence' then
  if not alive(app,ARGV[2],ARGV[3]) or redis.call('HGET',lease,'owner')~=ARGV[4] or
      redis.call('HGET',lease,'epoch')~=ARGV[5] or redis.call('HGET',lease,'appOwner')~=ARGV[2] or
      redis.call('HGET',lease,'appEpoch')~=ARGV[3] then return {'lost'} end
  redis.call('HDEL',lease,'owner','until','event','repairIntent');barrier();return {'ok'}
elseif op=='repair-release' then
  local e=work();if not e then return {'lost'} end
  local expected=cjson.decode(ARGV[9]);local currentRaw=redis.call('HGET',state,'session')
  if not currentRaw then return {'lost'} end
  local current=cjson.decode(currentRaw);if not sameBinding(expected,current) then return {'lost'} end
  redis.call('HDEL',lease,'owner','until','event','repairIntent');barrier();return {'ok'}
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
  local e=work();if not e then return {'lost'} end
  local t=cjson.decode(ARGV[9]);local suppliedSession=cjson.decode(ARGV[10])
  local stateSessionRaw=redis.call('HGET',state,'session');if not stateSessionRaw then return {'lost'} end
  local stateSession=cjson.decode(stateSessionRaw);local recovery=t.recovery
  if redis.call('HGET',state,'chat')~=t.checkpoint.canonicalChatId or not sameBinding(suppliedSession,stateSession) or
      not fields(recovery,6) or recovery.version~=1 or recovery.eventId~=ARGV[6] or
      recovery.eventFingerprint~=ARGV[8] or recovery.streamId~=ARGV[7] or not validGeneration(recovery.generation) or
      not sameBinding(recovery.session,stateSession) or e.fp~=recovery.eventFingerprint or e.id~=recovery.streamId then return {'lost'} end
  local old=redis.call('GET',checkpoint)
  if old then
    local p=cjson.decode(old)
    if p.checkpointId==t.checkpointId then if old~=ARGV[9] then return {'lost'} end
    elseif p.phase~='restored' and p.phase~='aborted' then return {'lost'} end
  end
  redis.call('SET',checkpoint,ARGV[9]);barrier();return {'ok'}
elseif op=='checkpoint-transition' then
  if not work() then return {'lost'} end
  local raw=redis.call('GET',checkpoint);if not raw then return {'lost'} end
  local t=cjson.decode(raw);if t.checkpointId~=ARGV[9] then return {'lost'} end
  if ARGV[10]=='compacted' and t.phase~='prepared' or ARGV[10]=='restored' and (t.phase~='compacted' or t.compactionEntryId~=ARGV[11]) then return {'lost'} end
  if raw~=ARGV[12] then return {'lost'} end
  redis.call('SET',checkpoint,ARGV[13]);barrier();return {'ok'}
elseif op=='checkpoint-abort' then
  local e=work();if not e then return {'lost'} end
  local expectedSession=cjson.decode(ARGV[9])
  local proof=repairEvidence(expectedSession.canonicalChatId,ARGV[14],ARGV[13]);if not proof then return {'lost'} end
  local event,provedEvent,currentSessionRaw,raw=proof[1],proof[2],proof[3],proof[4]
  local currentSession=cjson.decode(currentSessionRaw)
  if event~=ARGV[6] or provedEvent.id~=ARGV[7] or provedEvent.fp~=ARGV[8] or
      not sameBinding(expectedSession,currentSession) or raw~=ARGV[11] then return {'lost'} end
  local t=cjson.decode(raw);local n=cjson.decode(ARGV[12])
  if t.phase~='prepared' or t.checkpointId~=ARGV[10] or n.phase~='aborted' or
      n.checkpointId~=ARGV[10] or n.compactionEntryId then return {'lost'} end
  if not t.recovery and (ARGV[13]=='' or redis.call('HGET',lease,'repairIntent')~=ARGV[13]) then return {'lost'} end
  redis.call('SET',checkpoint,ARGV[12]);redis.call('HDEL',state,'pause')
  redis.call('HDEL',lease,'owner','until','event','repairIntent');barrier();return {'ok'}
elseif op=='checkpoint-latest' then return {redis.call('GET',checkpoint) or ''}
end
return redis.error_reply('store_unavailable')
`;
