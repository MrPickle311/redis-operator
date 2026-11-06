#!lua flags=no-writes,allow-cross-slot-keys
-- Samples up to 200 random keys and counts them by type and memory, for
-- the script of redis_exporter: pairs of "<type> <size>" and a count.
local sizes = {{1024, "<1KB"}, {10240, "1-10KB"}, {102400, "10-100KB"}, {1048576, "100KB-1MB"}}
local counts = {}
for _ = 1, 200 do
  local key = redis.call("RANDOMKEY")
  if not key then break end
  local size, class = redis.call("MEMORY", "USAGE", key) or 0, ">1MB"
  for _, s in ipairs(sizes) do
    if size < s[1] then class = s[2]; break end
  end
  local name = redis.call("TYPE", key)["ok"] .. " " .. class
  counts[name] = (counts[name] or 0) + 1
end
local out = {}
for name, n in pairs(counts) do
  out[#out + 1] = name
  out[#out + 1] = tostring(n)
end
return out
