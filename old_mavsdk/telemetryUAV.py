import asyncio
from channels.layers import get_channel_layer

################################################
################################################
# Telemetry controller for Drone with MavSDK
class drone_telemetry():

  # Variables
  position     = None
  flightMode   = None
  in_air       = None
  is_armed     = None
  posandvel    = None
  time         = None
  heading      = None
  quaternion   = None
  velocityNED  = None
  posBase      = None
  status       = None
  health       = None
  is_open      = True
  uav_attitude = None

  # Log
  pLog = None

  # For communication with JS
  try:
    channel_layer = get_channel_layer()
    groupName     = "uav"
    usingDjango   = True
  except: 
    usingDjango   = False

  # Tareas Iniciadas
  running_tasks = []

  def __init__(self, pLog, cooldown, drone_id):
      self.pLog     = pLog
      self.cooldown = cooldown
      self.drone_id = drone_id

  def run_tasks(self, drone):
      self.running_tasks = [
            # PRUEBA CON TODO EN UN MISMO ENSURE
          #asyncio.ensure_future( self.updateTelemetry(drone)    ),
          # CON TODOS SEPARADSO
          asyncio.ensure_future( self.getPosition(drone)    ),
          asyncio.ensure_future( self.getFlightMode(drone)  ),
          asyncio.ensure_future( self.getQuaternion(drone)  ),
          asyncio.ensure_future( self.getInAir(drone)       ),
          asyncio.ensure_future( self.getIsArmed(drone)     ),
          asyncio.ensure_future( self.getPosVel(drone)      ),
          asyncio.ensure_future( self.getTime(drone)        ),
          asyncio.ensure_future( self.getHeading(drone)     ),
          asyncio.ensure_future( self.getRotation(drone)    ),
          #asyncio.ensure_future( self.getTelemetry()        ),
      ]
  
  async def close_tasks(self):
      self.is_open = False
      for task in self.running_tasks:
          task.cancel()
          try:
              await task
          except asyncio.CancelledError:
              pass
        
  

  async def getPosition(self, drone):
      async for pos in drone.telemetry.position():
          self.position = pos

  async def getRotation(self, drone):
      async for uav_att in drone.telemetry.attitude_euler():
          self.uav_attitude = uav_att
  #####

  async def getIsArmed(self, drone):
      async for is_armed in drone.telemetry.armed():
          self.is_armed = is_armed
  #####

  async def getPosVel(self, drone):
      async for posandvel in drone.telemetry.position_velocity_ned():
          self.posandvel = posandvel
  #####

  async def getFlightMode(self, drone):
      async for flight_mode in drone.telemetry.flight_mode():
          self.flightMode = flight_mode
  #####

  async def getQuaternion(self, drone):
      async for quaternion in drone.telemetry.attitude_quaternion():
          self.quaternion = quaternion
  #####

  async def getInAir(self, drone):
      async for in_air in drone.telemetry.in_air():
          self.in_air = in_air
  #####

  async def getTime(self, drone):
    async for time in drone.telemetry.unix_epoch_time():
      self.time = time
  
  ###################
  # 
  async def getHeading(self, drone):
    async for heading in drone.telemetry.heading():
        self.heading = heading

  ###################
  # Toda la telemetria a la vez
  async def updateTelemetry(self, drone):
    while True:
        async for pos in drone.telemetry.position():
            self.position = pos
            break
        async for uav_att in drone.telemetry.attitude_euler():
            self.uav_attitude = uav_att
            break
        async for is_armed in drone.telemetry.armed():
            self.is_armed = is_armed
            break
        async for posandvel in drone.telemetry.position_velocity_ned():
            self.posandvel = posandvel
            break
        async for flight_mode in drone.telemetry.flight_mode():
            self.flightMode = flight_mode
            break
        async for quaternion in drone.telemetry.attitude_quaternion():
            self.quaternion = quaternion
            break
        async for in_air in drone.telemetry.in_air():
            self.in_air = in_air
            break
        async for time in drone.telemetry.unix_epoch_time():
            self.time = time
            break
        async for heading in drone.telemetry.heading():
            self.heading = heading
            break
        #return True

  async def getTelemetry(self):
      while self.is_open:
        await asyncio.sleep(self.cooldown)

        # Store the telemetry values at its corresponding array
        self.arrPosition.append(self.position)
        self.arrFlightMode.append(self.flightMode)
        self.arrIn_air.append(self.in_air)
        self.arrIs_armed.append(self.is_armed)
        self.arrPosandvel.append(self.posandvel)
        self.arrTime.append(self.time)
        self.arrHeading.append(self.heading)
        self.arrQuaternion.append(self.quaternion)
    
        live_telemetry = str(f"TELEMETRY:" 
               f"\n* {self.position}"+
            +  f"\n* FlightMode: {self.flightMode}"
               f"\n* In_air: {self.in_air}" +
               f"\n* Is_armed: {self.is_armed}" +
            +  f"\n* {self.posandvel.position}" 
            +  f"\n* {self.posandvel.velocity}"+
               f"\n* Time: {self.time}"+
               f"\n* Heading: {self.heading}"+
               f"\n* Quaternion: {self.quaternion}"+
            +  f"\n* Rotation: {self.uav_attitude}")
        self.pLog.info(live_telemetry)

        # Extract the info from the complex variables
        #if self.position is not None:
        #  latitude_deg        = self.position.latitude_deg
        #  longitude_deg       = self.position.longitude_deg
        #  absolute_altitude_m = self.position.absolute_altitude_m
        #  relative_altitude_m = self.position.relative_altitude_m
        #else:
        #  latitude_deg        = None
        #  longitude_deg       = None
        #  absolute_altitude_m = None
        #  relative_altitude_m = None
        #if self.heading is not None:
        #  heading_deg = self.heading.heading_deg    
        #else:
        #  heading_deg = None
        #if self.posandvel is not None:
        #  posNorth = self.posandvel.position.north_m
        #  posEast  = self.posandvel.position.east_m
        #  posDown  = self.posandvel.position.down_m
        #  velNorth = self.posandvel.velocity.north_m_s
        #  velEast  = self.posandvel.velocity.east_m_s
        #  velDown  = self.posandvel.velocity.down_m_s
        #else:
        #  posNorth = None
        #  posEast  = None
        #  posDown  = None
        #  velNorth = None
        #  velEast  = None
        #  velDown  = None
        #if self.quaternion is not None:
        #  wQuaternion = self.quaternion.w
        #  xQuaternion = self.quaternion.x
        #  yQuaternion = self.quaternion.y
        #  zQuaternion = self.quaternion.z
        #  timestampQuaternion = self.quaternion.timestamp_us
        #else:
        #  wQuaternion = None
        #  xQuaternion = None
        #  yQuaternion = None
        #  zQuaternion = None
        #  timestampQuaternion = None
        #if self.uav_attitude is not None:
        #  pitch_deg    = self.uav_attitude.pitch_deg
        #  roll_deg     = self.uav_attitude.roll_deg
        #  yaw_deg      = self.uav_attitude.yaw_deg
        #  timestamp_deg = self.uav_attitude.timestamp_us
        #else:
        #  pitch_deg     = None
        #  roll_deg      = None
        #  yaw_deg       = None
        #  timestamp_deg = None
        ## Si alguna de la telemetría es NULL, no se envía
        #if latitude_deg == None or longitude_deg == None or absolute_altitude_m == None or relative_altitude_m == None or             \
        #   wQuaternion == None or xQuaternion == None or yQuaternion == None or zQuaternion == None or timestampQuaternion == None or \
        #   posNorth == None or posEast == None or posDown == None or velNorth == None or velEast == None or velDown == None or heading_deg == None or \
        #   pitch_deg == None or roll_deg == None or yaw_deg == None or timestamp_deg == None:
        #  print("ALGO ES NONE")
        #else:
        #  # Send the info to Javascript (first to mainMap, and then to JS)
        #  message = {
        #    'type':       'messageToJS',
        #    'message':    'telemetry',
        #    'drone':      self.drone_id,
        #    'positionGlobal': {
        #      'latitude_deg': latitude_deg,
        #      'longitude_deg': longitude_deg,
        #      'absolute_altitude_m': absolute_altitude_m,
        #      'relative_altitude_m': relative_altitude_m,
        #    },
        #    'positionNED':  {
        #      'north': posNorth,
        #      'east':  posEast,
        #      'down':  posDown,
        #    },
        #    'velocityNED':  {
        #      'north': velNorth,
        #      'east':  velEast,
        #      'down':  velDown,
        #    },
        #    'orientation':  {
        #      'pitch_deg':     pitch_deg,
        #      'roll_deg':      roll_deg,
        #      'yaw_deg':       yaw_deg,
        #      'timestamp_deg': timestamp_deg,
        #    },
        #    'quaternion': {
        #      'wQuaternion': wQuaternion,
        #      'xQuaternion': xQuaternion,
        #      'yQuaternion': yQuaternion,
        #      'zQuaternion': zQuaternion,
        #      'timestampQuaternion': timestampQuaternion,
        #    },
        #    'flightMode': str( self.flightMode ),
        #    'in_air':     str( self.in_air ),
        #    'is_armed':   str( self.is_armed ),
        #    'time':       self.time,
        #    'heading':    heading_deg,
        #  }
        #  if self.usingDjango == True: await self.channel_layer.group_send(self.groupName, message)