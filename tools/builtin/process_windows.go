//go:build windows

package builtin

import (
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"
)

const jobObjectLimitKillOnJobClose = 0x2000

var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	createJobObject          = kernel32.NewProc("CreateJobObjectW")
	setInformationJobObject  = kernel32.NewProc("SetInformationJobObject")
	assignProcessToJobObject = kernel32.NewProc("AssignProcessToJobObject")
)

type jobObjectBasicLimitInformation struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}

type ioCounters struct {
	ReadOperationCount  uint64
	WriteOperationCount uint64
	OtherOperationCount uint64
	ReadTransferCount   uint64
	WriteTransferCount  uint64
	OtherTransferCount  uint64
}

type jobObjectExtendedLimitInformation struct {
	BasicLimitInformation jobObjectBasicLimitInformation
	IoInfo                ioCounters
	ProcessMemoryLimit    uintptr
	JobMemoryLimit        uintptr
	PeakProcessMemoryUsed uintptr
	PeakJobMemoryUsed     uintptr
}

type processHandle struct {
	cmd  *exec.Cmd
	job  syscall.Handle
	once sync.Once
	err  error
}

func startProcess(cmd *exec.Cmd) (*processHandle, error) {
	job, _, callErr := createJobObject.Call(0, 0)
	if job == 0 {
		return nil, callErr
	}
	p := &processHandle{cmd: cmd, job: syscall.Handle(job)}
	info := jobObjectExtendedLimitInformation{}
	info.BasicLimitInformation.LimitFlags = jobObjectLimitKillOnJobClose
	ok, _, callErr := setInformationJobObject.Call(uintptr(p.job), 9, uintptr(unsafe.Pointer(&info)), unsafe.Sizeof(info))
	if ok == 0 {
		_ = p.Close()
		return nil, callErr
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x00000008 | 0x01000000}
	if err := cmd.Start(); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("builtin: start shell: %w", err)
	}
	process, err := syscall.OpenProcess(0x0200|0x0001, false, uint32(cmd.Process.Pid))
	if err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = p.Close()
		return nil, fmt.Errorf("builtin: open shell process: %w", err)
	}
	ok, _, callErr = assignProcessToJobObject.Call(uintptr(p.job), uintptr(process))
	closeErr := syscall.CloseHandle(process)
	if ok == 0 {
		// 某些宿主 Job 禁止再次分配；保留运行中的 shell，取消时改用
		// taskkill 终止整棵进程树，而不是让工具无法启动。
		_ = p.Close()
		return p, nil
	}
	if closeErr != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = p.Close()
		return nil, closeErr
	}
	return p, nil
}

func (p *processHandle) Kill() error {
	var err error
	if p.job == 0 && p.cmd.Process != nil {
		err = exec.Command("taskkill", "/T", "/F", "/PID", fmt.Sprint(p.cmd.Process.Pid)).Run()
	} else {
		err = p.Close()
	}
	if p.cmd.Process != nil {
		if killErr := p.cmd.Process.Kill(); err == nil {
			err = killErr
		}
	}
	return err
}

func (p *processHandle) Close() error {
	p.once.Do(func() {
		if p.job != 0 {
			p.err = syscall.CloseHandle(p.job)
			p.job = 0
		}
	})
	return p.err
}
