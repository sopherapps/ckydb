package internal

type Storage interface {
	Load() error
	Set(key string, value string) error
	Get(key string) (string, error)
	Delete(key string) error
	Clear() error
	Vacuum() error
}

type Store struct {
}

func (s *Store) Load() error {
	panic("implement me")
}

func (s *Store) Set(key string, value string) error {
	panic("implement me")
}

func (s *Store) Get(key string) (string, error) {
	panic("implement me")
}

func (s *Store) Delete(key string) error {
	panic("implement me")
}

func (s *Store) Clear() error {
	panic("implement me")
}

func (s *Store) Vacuum() error {
	panic("implement me")
}
