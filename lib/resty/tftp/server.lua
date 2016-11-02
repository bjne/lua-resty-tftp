local match = string.match
local floor = math.floor
local _C = string.char

local _M = { _VERSION = 0.1 }

local op = {
    [_C(0,1)] = 'RRQ',  RRQ  = _C(0,1),
    [_C(0,2)] = 'WRQ',  WRQ  = _C(0,2),
    [_C(0,3)] = 'DATA', DATA = _C(0,3),
    [_C(0,4)] = 'ACK',  ACK  = _C(0,4),
    [_C(0,5)] = 'ERR',  ERR  = _C(0,5)
}

_M.serve = function(path)
    local sock = ngx.req.udp_socket()
    if not sock then
        return ngx.log(ngx.WARN, "no socket")
    end

    local addr, port = ngx.var.remote_addr, ngx.var.remote_port

    sock:settimeout(5000)

    local data, err = sock:receive(256)
    if not data then
        return ngx.log(ngx.WARN, "failed to read packet")
    end

    local opcode, file, mode = match(data, '^(%z.)([^%s]+)%z([^%z]+)%z')
    if not opcode or op[opcode] ~= 'RRQ' then
        return nil, "only read supported"
    end

    if match(file, '/') then
        return nil, "/ in filename can be dangerous, unsupported"
    end

    --[[
        at this point we are done with the port 69 socket
        we need to create a new udp socket from a random
        source port to client on the port it connected from
    --]]

    sock = ngx.socket.udp()
    if not sock then
        return nil, "ngx.socket.udp() failed"
    end

    local ok, err = sock:setpeername(addr, port)
    if not ok then
        return nil, "setpeername(host, port) failed: "..err
    end

    if path then
        file = path..'/'..file
    end

    local f, err = io.open(file)
    if not f then
        return nil, err
    end

    local function send_data_block(data, block)
        data = data or ''
        for i=1,5 do
            sock:send(op.DATA .. _C(floor(block/256), block%256) .. data)
            local ack, err = sock:receive(32)
            if ack and #ack == 4 then
                local opcode, _ = match(ack, '^(%z.)(..)')
                if not opcode or not op[opcode] == 'ACK' then
                    ngx.log(ngx.WARN, "unknown reply for block: ", block)
                else
                    return true
                end
            elseif ack then
                -- TODO: handle error message
            else
                ngx.log(ngx.WARN, "timeout receiving reply for block: ", block)
            end
        end
    end

    for block=1,65535 do
        local data = f:read(512)

        local ok = send_data_block(data, block)
        if not ok or not data then
            return
        end
    end
end

return _M

-- vim: ts=4 sw=4 et ai
