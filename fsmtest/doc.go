// Package fsmtest provides fixtures for testing code built on fsm against both storage backends
// without external services: a manager factory over a temp BoltDB or an in-memory S3, helpers
// for the lease timings that make a takeover deterministic, and a polling assertion.
//
// A restart-and-resume scenario runs the same way on either backend:
//
//	func TestDeployResumes(t *testing.T) {
//		fsmtest.RunBackends(t, func(t *testing.T, f *fsmtest.Factory) {
//			m1, stop1 := f.NewManager(nil)
//			// register, start a run, let it block ...
//			stop1()
//			m2, _ := f.NewManager(nil)
//			// register again, Resume, Wait ...
//		})
//	}
//
// The fakes themselves live in the fsmtest/fake subpackage, which does not import fsm; a test
// that builds its own fsm.Config can take fake.NewS3(t).Client() and fake.NewBus() directly.
package fsmtest
