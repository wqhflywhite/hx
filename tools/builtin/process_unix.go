//go:build !windows

package builtin

import (
	"os/exec"
	"syscall"
)

type processHandle struct{ cmd *exec.Cmd }

func startProcess(cmd *exec.Cmd) (*processHandle, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &processHandle{cmd: cmd}, nil
}

func (p *processHandle) Kill() error {
	if p.cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL); err != nil {
		return p.cmd.Process.Kill()
	}
	return nil
}

func (p *processHandle) Close() error { return nil }
