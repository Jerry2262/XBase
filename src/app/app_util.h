#pragma once

#include <functional>
#include <ostream>
#include <string>

#include "config/config.h"
#include "status.h"

class Server;

namespace AppUtil {

using StopCallback = std::function<void()>;
using IsStoppedCallback = std::function<bool()>;

CLIOptions ParseCommandLineOptions(int argc, char **argv);
ServiceType DetectServiceType(const std::string &program_name);
void PrintVersion(std::ostream &os);
void InitGoogleLog(const Config *config);
void SetupSignalHandlers();
void RegisterStopHandler(IsStoppedCallback is_stopped, StopCallback stop);
void ClearStopHandler();
void SetServer(Server *srv);
bool IsSupervisedMode(int mode);
Status CreatePidFile(const std::string &path);
void RemovePidFile(const std::string &path);
void Daemonize();

}  // namespace AppUtil

Server *GetServer();
