// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

namespace RCM {

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

class Controller {
	public:
	Controller(){}
	virtual ~Controller(){}
};

} // namespace RCM

#define DECLARE_RCM_POINTER \
  private: \
  std::unique_ptr<RCM::Controller> rcm_controller
