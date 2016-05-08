local name = ARGV[1]
local partition = "eventr:partitions:" .. ARGV[2]
local coutnerKey = "eventr:counters:" .. ARGV[2]

-- If we have a key already for this name, look it up
local counter = 0
if redis.call("HEXISTS", coutnerKey, name) == 1 then
  counter = redis.call("HGET", coutnerKey, name)
  counter = tonumber(counter)
end

-- if the partition exists, get the data from that key
if redis.call("EXISTS", partition) == 0 then
  return nil
else
  local event = redis.call("LRANGE", partition, counter, counter)
  if (event and  event[1] ~= nil) then
    redis.call("HSET", coutnerKey, name, (counter + 1))
    return event[1]
  else
    return nil
  end
end
